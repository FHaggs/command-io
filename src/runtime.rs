use std::collections::VecDeque;

use crate::arena::{Arena, ArenaError, Handle};
use crate::effects::{Action, EffectsError, RuntimeMessage, TurnEffects, Wait};
use crate::io::{FakeWaitBackend, WaitBackend};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope<M> {
    pub target: Handle,
    pub message: RuntimeMessage<M>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Arena(ArenaError),
    Effects(EffectsError),
    QueueFull,
    WaitQueueFull,
}

impl From<ArenaError> for RuntimeError {
    fn from(value: ArenaError) -> Self {
        Self::Arena(value)
    }
}

impl From<EffectsError> for RuntimeError {
    fn from(value: EffectsError) -> Self {
        Self::Effects(value)
    }
}

pub trait Isolate {
    type Message;

    fn handle(
        &mut self,
        msg: RuntimeMessage<Self::Message>,
        effects: &mut TurnEffects<Self::Message>,
    ) -> Result<(), RuntimeError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    Idle,
    ProcessedOne,
    DroppedInvalid,
    AdvancedWaits,
}

pub struct Scheduler<I, B>
where
    I: Isolate,
    B: WaitBackend<I::Message>,
{
    arena: Arena<I>,
    queue: VecDeque<Envelope<I::Message>>,
    queue_capacity: usize,
    effects: TurnEffects<I::Message>,
    action_scratch: Vec<Action<I::Message>>,
    interpreter: EffectInterpreter,
    waits: B,
}

pub struct EffectInterpreter;

impl EffectInterpreter {
    fn interpret_turn<I, B>(
        &mut self,
        arena: &mut Arena<I>,
        queue: &mut VecDeque<Envelope<I::Message>>,
        queue_capacity: usize,
        waits: &mut B,
        current: Handle,
        actions: &mut Vec<Action<I::Message>>,
        wait: Option<Wait<I::Message>>,
    ) -> Result<(), RuntimeError>
    where
        I: Isolate,
        B: WaitBackend<I::Message>,
    {
        for action in actions.drain(..) {
            match action {
                Action::Send { target, message } => {
                    enqueue_envelope(queue, queue_capacity, target, message)?;
                }
                Action::SendSelf { message } => {
                    enqueue_envelope(queue, queue_capacity, current, RuntimeMessage::User(message))?;
                }
                Action::DestroySelf => {
                    if arena.contains(current) {
                        let _ = arena.remove(current)?;
                    }
                }
            }
        }

        if let Some(wait) = wait {
            waits.submit(current, wait)?;
        }

        Ok(())
    }
}

impl<I> Scheduler<I, FakeWaitBackend<I::Message>>
where
    I: Isolate,
{
    pub fn new(arena_capacity: usize, queue_capacity: usize, action_capacity: usize) -> Self {
        Self::new_with_backend(
            arena_capacity,
            queue_capacity,
            action_capacity,
            FakeWaitBackend::new(queue_capacity),
        )
    }
}

impl<I, B> Scheduler<I, B>
where
    I: Isolate,
    B: WaitBackend<I::Message>,
{
    pub fn new_with_backend(
        arena_capacity: usize,
        queue_capacity: usize,
        action_capacity: usize,
        waits: B,
    ) -> Self {
        Self {
            arena: Arena::with_capacity(arena_capacity),
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
            effects: TurnEffects::with_capacity(action_capacity),
            action_scratch: Vec::with_capacity(action_capacity),
            interpreter: EffectInterpreter,
            waits,
        }
    }

    pub fn spawn(&mut self, isolate: I) -> Result<Handle, RuntimeError> {
        let handle = self.arena.insert(isolate)?;

        if let Err(error) = self.enqueue_runtime(handle, RuntimeMessage::Init) {
            let _ = self.arena.remove(handle);
            return Err(error);
        }

        Ok(handle)
    }

    pub fn enqueue(&mut self, target: Handle, message: I::Message) -> Result<(), RuntimeError> {
        self.enqueue_runtime(target, RuntimeMessage::User(message))
    }

    pub fn enqueue_runtime(
        &mut self,
        target: Handle,
        message: RuntimeMessage<I::Message>,
    ) -> Result<(), RuntimeError> {
        enqueue_envelope(&mut self.queue, self.queue_capacity, target, message)
    }

    pub fn run_once(&mut self) -> Result<StepResult, RuntimeError> {
        let wait_progress = self.waits.poll(&mut self.queue, self.queue_capacity)?;
        let Some(envelope) = self.queue.pop_front() else {
            return if wait_progress || self.waits.has_pending() {
                Ok(StepResult::AdvancedWaits)
            } else {
                Ok(StepResult::Idle)
            };
        };

        if !self.arena.contains(envelope.target) {
            return Ok(StepResult::DroppedInvalid);
        }

        self.effects.reset();
        {
            let isolate = self
                .arena
                .get_mut(envelope.target)
                .ok_or(RuntimeError::Arena(ArenaError::InvalidHandle))?;
            isolate.handle(envelope.message, &mut self.effects)?;
        }

        self.effects.swap_actions(&mut self.action_scratch);
        let wait = self.effects.take_wait();
        self.interpreter.interpret_turn(
            &mut self.arena,
            &mut self.queue,
            self.queue_capacity,
            &mut self.waits,
            envelope.target,
            &mut self.action_scratch,
            wait,
        )?;

        Ok(StepResult::ProcessedOne)
    }

    pub fn run_until_idle(&mut self) -> Result<usize, RuntimeError> {
        let mut processed = 0;

        loop {
            match self.run_once()? {
                StepResult::Idle => return Ok(processed),
                StepResult::ProcessedOne => processed += 1,
                StepResult::DroppedInvalid | StepResult::AdvancedWaits => {}
            }
        }
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn isolate_count(&self) -> usize {
        self.arena.len()
    }

    pub fn isolate_mut(&mut self, handle: Handle) -> Option<&mut I> {
        self.arena.get_mut(handle)
    }
}

fn enqueue_envelope<M>(
    queue: &mut VecDeque<Envelope<M>>,
    queue_capacity: usize,
    target: Handle,
    message: RuntimeMessage<M>,
) -> Result<(), RuntimeError> {
    if queue.len() == queue_capacity {
        return Err(RuntimeError::QueueFull);
    }

    queue.push_back(Envelope { target, message });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Isolate, RuntimeError, Scheduler, StepResult};
    use crate::effects::{EffectsError, RuntimeMessage, TurnEffects};

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
        assert_eq!(scheduler.isolate_count(), 0);
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

        assert_eq!(scheduler.isolate_count(), 2);
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
        assert_eq!(scheduler.isolate_count(), 0);
        assert_eq!(scheduler.queue_len(), 0);
    }

    #[test]
    fn wait_seals_the_turn_effects() {
        let mut effects = TurnEffects::<Msg>::with_capacity(2);

        effects.wait_accept(1, Msg::Accepted).unwrap();

        assert_eq!(effects.send_self(Msg::Tick), Err(EffectsError::TurnSealed));
    }
}