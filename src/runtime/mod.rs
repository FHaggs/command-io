mod envelope;
mod error;
mod inflight;
mod interpreter;
mod isolate;
mod scheduler;
mod step;

pub use error::RuntimeError;
pub use isolate::Isolate;
pub use scheduler::Scheduler;

#[cfg(test)]
mod tests {
    use super::step::StepResult;
    use super::{Isolate, RuntimeError, Scheduler};
    use crate::arena::Handle;
    use crate::effects::{OpToken, RuntimeMessage, TurnEffects, Wait};
    use crate::io::WaitBackend;

    #[derive(Debug)]
    struct Countdown {
        remaining: u8,
    }

    #[derive(Debug)]
    struct Relay {
        peer: Option<crate::arena::Handle>,
        seen_ping: bool,
        seen_pong: bool,
    }

    #[derive(Debug)]
    struct AcceptOnce {
        accepted: bool,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Msg {
        Tick,
        Ping,
        Pong,
        Accepted,
    }

    impl Isolate for Countdown {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            _token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    effects.send_self(Msg::Tick)?;
                }
                RuntimeMessage::User(Msg::Tick) => {
                    self.remaining -= 1;

                    if self.remaining == 0 {
                        effects.destroy_self()?;
                    } else {
                        effects.send_self(Msg::Tick)?;
                    }
                }
                RuntimeMessage::User(Msg::Ping)
                | RuntimeMessage::User(Msg::Pong)
                | RuntimeMessage::User(Msg::Accepted) => {
                    unreachable!("countdown only processes ticks")
                }
            }

            Ok(())
        }
    }

    impl Isolate for Relay {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            _token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {}
                RuntimeMessage::User(Msg::Ping) => {
                    self.seen_ping = true;
                    let peer = self.peer.expect("peer handle must be assigned");
                    effects.send(peer, Msg::Pong)?;
                }
                RuntimeMessage::User(Msg::Pong) => {
                    self.seen_pong = true;
                }
                RuntimeMessage::User(Msg::Tick) | RuntimeMessage::User(Msg::Accepted) => {
                    unreachable!("relay only processes ping and pong")
                }
            }

            Ok(())
        }
    }

    impl Isolate for AcceptOnce {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            _token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    effects.wait_accept(7, Msg::Accepted)?;
                }
                RuntimeMessage::User(Msg::Accepted) => {
                    self.accepted = true;
                    effects.destroy_self()?;
                }
                RuntimeMessage::User(Msg::Tick)
                | RuntimeMessage::User(Msg::Ping)
                | RuntimeMessage::User(Msg::Pong) => {
                    unreachable!("accept-once only processes acceptance")
                }
            }

            Ok(())
        }
    }

    #[test]
    fn scheduler_runs_until_isolate_destroys_itself() {
        let mut scheduler = Scheduler::new(4, 8, 4);
        let isolate = Countdown { remaining: 3 };

        let _handle = scheduler.spawn(isolate).unwrap();

        let processed = scheduler.run_until_idle().unwrap();

        assert_eq!(processed, 4);
        assert_eq!(scheduler.queue_len(), 0);
    }

    #[test]
    fn queue_capacity_applies_backpressure() {
        let mut scheduler = Scheduler::new(1, 1, 1);
        let isolate = Countdown { remaining: 1 };

        let handle = scheduler.spawn(isolate).unwrap();

        assert_eq!(scheduler.enqueue(handle, Msg::Tick), Err(RuntimeError::QueueFull));
    }

    #[test]
    fn spawn_enqueues_runtime_init_automatically() {
        let mut scheduler = Scheduler::new(1, 4, 2);
        let isolate = Countdown { remaining: 1 };

        let _handle = scheduler.spawn(isolate).unwrap();

        assert_eq!(scheduler.run_once().unwrap(), StepResult::ProcessedOne);
        assert_eq!(scheduler.queue_len(), 1);
    }

    #[test]
    fn isolates_can_send_messages_to_other_isolates() {
        let mut scheduler = Scheduler::new(4, 8, 4);
        let left = Relay {
            peer: None,
            seen_ping: false,
            seen_pong: false,
        };
        let right = Relay {
            peer: None,
            seen_ping: false,
            seen_pong: false,
        };

        let left_handle = scheduler.spawn(left).unwrap();
        let right_handle = scheduler.spawn(right).unwrap();
        scheduler.isolate_mut(left_handle).unwrap().peer = Some(right_handle);
        scheduler.isolate_mut(right_handle).unwrap().peer = Some(left_handle);

        scheduler.enqueue(left_handle, Msg::Ping).unwrap();
        scheduler.run_until_idle().unwrap();

        assert!(scheduler.isolate_mut(left_handle).unwrap().seen_ping);
        assert!(scheduler.isolate_mut(right_handle).unwrap().seen_pong);
    }

    #[test]
    fn fake_accept_wait_yields_and_resumes_on_future_turn() {
        let mut scheduler = Scheduler::new(1, 4, 2);
        let isolate = AcceptOnce { accepted: false };

        let _handle = scheduler.spawn(isolate).unwrap();

        let processed = scheduler.run_until_idle().unwrap();

        assert_eq!(processed, 2);
        assert_eq!(scheduler.queue_len(), 0);
    }

    #[test]
    fn wait_seals_the_turn_effects() {
        let mut effects = TurnEffects::<Msg>::with_capacity(2, 2);

        // Arming multiple waits in one turn is now allowed (a wait set); each
        // returns a distinct token.
        let a = effects.wait_accept(1, Msg::Accepted).unwrap();
        let b = effects.wait_recv(2, Msg::Accepted).unwrap();

        assert_ne!(a, b);
        // Ordinary actions can still be added after arming waits.
        effects.send_self(Msg::Tick).unwrap();
    }

    // --- Phase 0 additions ---------------------------------------------------

    /// Arms a recv wait, remembers the returned token, and records the token
    /// delivered on the completion so the test can assert they match.
    #[derive(Debug)]
    struct RecvCorrelate {
        armed_token: Option<OpToken>,
        completed_token: Option<OpToken>,
    }

    impl Isolate for RecvCorrelate {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    self.armed_token = Some(effects.wait_recv(3, Msg::Accepted)?);
                }
                RuntimeMessage::User(Msg::Accepted) => {
                    self.completed_token = token;
                }
                RuntimeMessage::User(Msg::Tick)
                | RuntimeMessage::User(Msg::Ping)
                | RuntimeMessage::User(Msg::Pong) => {
                    unreachable!("recv-correlate only processes acceptance")
                }
            }

            Ok(())
        }
    }

    /// On `Init` emits two self-sends; used to force a mid-turn `QueueFull`.
    #[derive(Debug)]
    struct FloodSelf;

    impl Isolate for FloodSelf {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            _token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    effects.send_self(Msg::Tick)?;
                    effects.send_self(Msg::Tick)?;
                }
                RuntimeMessage::User(Msg::Tick) => {}
                RuntimeMessage::User(Msg::Ping)
                | RuntimeMessage::User(Msg::Pong)
                | RuntimeMessage::User(Msg::Accepted) => {
                    unreachable!("flood-self only processes ticks")
                }
            }

            Ok(())
        }
    }

    /// A backend that accepts waits but never completes them, to prove the
    /// scheduler reports `Blocked` and `run_until_idle` terminates instead of
    /// busy-spinning.
    struct StuckBackend {
        pending: u32,
    }

    impl WaitBackend for StuckBackend {
        fn submit(&mut self, _token: OpToken, _wait: Wait) -> Result<(), RuntimeError> {
            self.pending += 1;
            Ok(())
        }

        fn cancel(&mut self, _token: OpToken) {
            self.pending = self.pending.saturating_sub(1);
        }

        fn poll(&mut self, _ready: &mut Vec<OpToken>) -> Result<bool, RuntimeError> {
            Ok(false)
        }

        fn has_pending(&self) -> bool {
            self.pending > 0
        }

        fn can_submit(&self, _additional: usize) -> bool {
            true
        }
    }

    #[test]
    fn completion_carries_the_matching_op_token() {
        let mut scheduler = Scheduler::new(1, 4, 2);
        let handle = scheduler
            .spawn(RecvCorrelate {
                armed_token: None,
                completed_token: None,
            })
            .unwrap();

        scheduler.run_until_idle().unwrap();

        let isolate = scheduler.isolate_mut(handle).unwrap();
        assert!(isolate.armed_token.is_some());
        assert_eq!(isolate.armed_token, isolate.completed_token);
    }

    #[test]
    fn queue_full_leaves_effects_unapplied() {
        // Queue capacity 1: `Init` is popped, then two self-sends cannot both
        // fit, so the whole turn must be rejected with nothing enqueued.
        let mut scheduler = Scheduler::new(1, 1, 4);
        let _handle = scheduler.spawn(FloodSelf).unwrap();

        assert_eq!(scheduler.run_once(), Err(RuntimeError::QueueFull));
        assert_eq!(scheduler.queue_len(), 0);
    }

    #[test]
    fn stuck_wait_reports_blocked() {
        let mut scheduler = Scheduler::new_with_backend(1, 4, 2, StuckBackend { pending: 0 });
        let _handle = scheduler.spawn(AcceptOnce { accepted: false }).unwrap();

        assert_eq!(scheduler.run_once().unwrap(), StepResult::ProcessedOne);
        assert_eq!(scheduler.run_once().unwrap(), StepResult::Blocked);
    }

    #[test]
    fn run_until_idle_does_not_spin_on_stuck_wait() {
        let mut scheduler = Scheduler::new_with_backend(1, 4, 2, StuckBackend { pending: 0 });
        let _handle = scheduler.spawn(AcceptOnce { accepted: false }).unwrap();

        // Only `Init` runs; the armed wait never completes, so this must return
        // rather than loop forever.
        let processed = scheduler.run_until_idle().unwrap();
        assert_eq!(processed, 1);
    }

    // --- Phase 1 additions ---------------------------------------------------

    /// Arms two independent waits on `Init` and counts how many completions
    /// arrive, proving a single turn can hold more than one in-flight op.
    #[derive(Debug)]
    struct TwoWaits {
        completions: u8,
    }

    impl Isolate for TwoWaits {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            _token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    effects.wait_recv(1, Msg::Ping)?;
                    effects.wait_recv(2, Msg::Pong)?;
                }
                RuntimeMessage::User(Msg::Ping) | RuntimeMessage::User(Msg::Pong) => {
                    self.completions += 1;
                }
                RuntimeMessage::User(_) => unreachable!("two-waits only sees ping/pong"),
            }

            Ok(())
        }
    }

    /// Arms two waits then immediately cancels the second; only one completion
    /// should ever be delivered.
    #[derive(Debug)]
    struct CancelSecond {
        second: Option<OpToken>,
        completions: u8,
    }

    impl Isolate for CancelSecond {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            _token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    effects.wait_recv(1, Msg::Ping)?;
                    let second = effects.wait_recv(2, Msg::Pong)?;
                    self.second = Some(second);
                    effects.cancel(second)?;
                }
                RuntimeMessage::User(Msg::Ping) | RuntimeMessage::User(Msg::Pong) => {
                    self.completions += 1;
                }
                RuntimeMessage::User(_) => unreachable!("cancel-second only sees ping/pong"),
            }

            Ok(())
        }
    }

    /// Races a recv against a timer via `select`; records which token won.
    #[derive(Debug)]
    struct SelectRace {
        first_member: Option<OpToken>,
        winner: Option<OpToken>,
        completions: u8,
    }

    impl Isolate for SelectRace {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    let tokens = effects.select([
                        (Wait::Recv { source: 1 }, Msg::Ping),
                        (Wait::Timer { ticks: 5 }, Msg::Tick),
                    ])?;
                    self.first_member = Some(tokens[0]);
                }
                RuntimeMessage::User(Msg::Ping)
                | RuntimeMessage::User(Msg::Tick)
                | RuntimeMessage::User(Msg::Pong)
                | RuntimeMessage::User(Msg::Accepted) => {
                    self.completions += 1;
                    self.winner = token;
                }
            }

            Ok(())
        }
    }

    /// Joins a recv and a timer; the single join completion should fire exactly
    /// once, after both members finish.
    #[derive(Debug)]
    struct JoinAll {
        join_token: Option<OpToken>,
        completed_token: Option<OpToken>,
        completions: u8,
    }

    impl Isolate for JoinAll {
        type Message = Msg;

        fn handle(
            &mut self,
            msg: RuntimeMessage<Self::Message>,
            token: Option<OpToken>,
            effects: &mut TurnEffects<Self::Message>,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    let join = effects.join(
                        [Wait::Recv { source: 1 }, Wait::Timer { ticks: 3 }],
                        Msg::Accepted,
                    )?;
                    self.join_token = Some(join);
                }
                RuntimeMessage::User(Msg::Accepted) => {
                    self.completions += 1;
                    self.completed_token = token;
                }
                RuntimeMessage::User(_) => {
                    unreachable!("join members are suppressed; only the join fires")
                }
            }

            Ok(())
        }
    }

    #[test]
    fn multiple_independent_waits_all_complete() {
        let mut scheduler = Scheduler::new(1, 8, 4);
        let handle = scheduler.spawn(TwoWaits { completions: 0 }).unwrap();

        scheduler.run_until_idle().unwrap();

        assert_eq!(scheduler.isolate_mut(handle).unwrap().completions, 2);
    }

    #[test]
    fn cancel_prevents_a_completion() {
        let mut scheduler = Scheduler::new(1, 8, 4);
        let handle = scheduler
            .spawn(CancelSecond {
                second: None,
                completions: 0,
            })
            .unwrap();

        scheduler.run_until_idle().unwrap();

        // Only the un-cancelled recv completes.
        assert_eq!(scheduler.isolate_mut(handle).unwrap().completions, 1);
    }

    #[test]
    fn select_delivers_winner_and_cancels_losers() {
        let mut scheduler = Scheduler::new(1, 8, 4);
        let handle = scheduler
            .spawn(SelectRace {
                first_member: None,
                winner: None,
                completions: 0,
            })
            .unwrap();

        scheduler.run_until_idle().unwrap();

        let isolate = scheduler.isolate_mut(handle).unwrap();
        // The recv fires before the 5-tick timer, so exactly one completion is
        // delivered and it is the first (recv) member.
        assert_eq!(isolate.completions, 1);
        assert_eq!(isolate.winner, isolate.first_member);
    }

    #[test]
    fn join_fires_once_after_all_members_complete() {
        let mut scheduler = Scheduler::new(1, 8, 4);
        let handle = scheduler
            .spawn(JoinAll {
                join_token: None,
                completed_token: None,
                completions: 0,
            })
            .unwrap();

        scheduler.run_until_idle().unwrap();

        let isolate = scheduler.isolate_mut(handle).unwrap();
        assert_eq!(isolate.completions, 1);
        assert_eq!(isolate.completed_token, isolate.join_token);
    }
}
