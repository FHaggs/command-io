mod arena;
mod effects;
mod io;
mod runtime;

use effects::{OpToken, RuntimeMessage, TurnEffects, Wait};
use runtime::{Isolate, RuntimeError, Scheduler};

/// A small connection-like isolate that arms one asynchronous operation per
/// turn, remembers the [`OpToken`] the runtime hands back, and correlates each
/// completion to the operation that produced it using the delivered token.
#[derive(Debug)]
struct Connection {
    accept_token: Option<OpToken>,
    recv_token: Option<OpToken>,
    write_token: Option<OpToken>,
}

impl Connection {
    fn new() -> Self {
        Self {
            accept_token: None,
            recv_token: None,
            write_token: None,
        }
    }

    /// Human-readable label for a completed operation, resolved by token.
    fn label_for(&self, token: Option<OpToken>) -> &'static str {
        if token == self.accept_token {
            "accept"
        } else if token == self.recv_token {
            "recv"
        } else if token == self.write_token {
            "write"
        } else {
            "unknown"
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ConnEvent {
    Accepted,
    Received,
    Written,
}

impl Isolate for Connection {
    type Message = ConnEvent;

    fn handle(
        &mut self,
        msg: RuntimeMessage<Self::Message>,
        token: Option<OpToken>,
        effects: &mut TurnEffects<Self::Message>,
    ) -> Result<(), RuntimeError> {
        match msg {
            RuntimeMessage::Init => {
                // Arm an accept. The runtime returns a token we keep so the
                // completion can be matched back to this exact operation.
                let armed = effects.wait_accept(1, ConnEvent::Accepted)?;
                println!("init: armed accept -> token={}", armed.0);
                self.accept_token = Some(armed);
            }
            RuntimeMessage::User(ConnEvent::Accepted) => {
                println!(
                    "completed op '{}' (token={:?}); arming recv",
                    self.label_for(token),
                    token.map(|t| t.0),
                );
                let armed = effects.wait_recv(1, ConnEvent::Received)?;
                println!("armed recv -> token={}", armed.0);
                self.recv_token = Some(armed);
            }
            RuntimeMessage::User(ConnEvent::Received) => {
                println!(
                    "completed op '{}' (token={:?}); arming write",
                    self.label_for(token),
                    token.map(|t| t.0),
                );
                let armed = effects.wait_write(1, ConnEvent::Written)?;
                println!("armed write -> token={}", armed.0);
                self.write_token = Some(armed);
            }
            RuntimeMessage::User(ConnEvent::Written) => {
                println!(
                    "completed op '{}' (token={:?}); closing connection",
                    self.label_for(token),
                    token.map(|t| t.0),
                );
                effects.destroy_self()?;
            }
        }

        Ok(())
    }
}

/// Races two operations with `select`: a receive versus a timeout. The first to
/// complete wins and the runtime cancels the loser automatically.
#[derive(Debug)]
struct Racer {
    recv_token: Option<OpToken>,
    timeout_token: Option<OpToken>,
}

#[derive(Clone, Copy, Debug)]
enum RaceEvent {
    Received,
    TimedOut,
}

impl Isolate for Racer {
    type Message = RaceEvent;

    fn handle(
        &mut self,
        msg: RuntimeMessage<Self::Message>,
        token: Option<OpToken>,
        effects: &mut TurnEffects<Self::Message>,
    ) -> Result<(), RuntimeError> {
        match msg {
            RuntimeMessage::Init => {
                // Arm a race: whichever of these completes first wins; the other
                // is cancelled by the runtime before its completion is delivered.
                let tokens = effects.select([
                    (Wait::Recv { source: 2 }, RaceEvent::Received),
                    (Wait::Timer { ticks: 4 }, RaceEvent::TimedOut),
                ])?;
                self.recv_token = Some(tokens[0]);
                self.timeout_token = Some(tokens[1]);
                println!(
                    "racer: armed select recv=token={} timeout=token={}",
                    tokens[0].0, tokens[1].0
                );
            }
            RuntimeMessage::User(event) => {
                let label = if token == self.recv_token {
                    "recv"
                } else if token == self.timeout_token {
                    "timeout"
                } else {
                    "unknown"
                };
                println!("racer: {label:?} won the race ({event:?}); loser cancelled");
                let _ = label;
                effects.destroy_self()?;
            }
        }

        Ok(())
    }
}

/// Waits for several operations to all finish with `join`, receiving a single
/// completion once the last member is done.
#[derive(Debug)]
struct Fanout {
    join_token: Option<OpToken>,
}

#[derive(Clone, Copy, Debug)]
enum FanoutEvent {
    AllReady,
}

impl Isolate for Fanout {
    type Message = FanoutEvent;

    fn handle(
        &mut self,
        msg: RuntimeMessage<Self::Message>,
        token: Option<OpToken>,
        effects: &mut TurnEffects<Self::Message>,
    ) -> Result<(), RuntimeError> {
        match msg {
            RuntimeMessage::Init => {
                // Fan out three reads; individual completions are suppressed and
                // a single join completion arrives once all three finish.
                let join = effects.join(
                    [
                        Wait::Recv { source: 3 },
                        Wait::Recv { source: 4 },
                        Wait::Timer { ticks: 2 },
                    ],
                    FanoutEvent::AllReady,
                )?;
                self.join_token = Some(join);
                println!("fanout: armed join -> join token={}", join.0);
            }
            RuntimeMessage::User(FanoutEvent::AllReady) => {
                println!(
                    "fanout: all members complete (join token={:?}); done",
                    token.map(|t| t.0)
                );
                effects.destroy_self()?;
            }
        }

        Ok(())
    }
}

fn main() {
    let mut scheduler = Scheduler::new(16, 64, 8);

    scheduler
        .spawn(Connection::new())
        .expect("spawn connection isolate");

    let processed = scheduler
        .run_until_idle()
        .expect("runtime should drain queue");
    println!("processed {processed} connection items\n");

    // Demonstrate select (race) with a dedicated scheduler run.
    let mut race_scheduler = Scheduler::new(4, 32, 8);
    race_scheduler
        .spawn(Racer {
            recv_token: None,
            timeout_token: None,
        })
        .expect("spawn racer isolate");
    let raced = race_scheduler
        .run_until_idle()
        .expect("race runtime should drain queue");
    println!("processed {raced} race items\n");

    // Demonstrate join (fan-in) with a dedicated scheduler run.
    let mut join_scheduler = Scheduler::new(4, 32, 8);
    join_scheduler
        .spawn(Fanout { join_token: None })
        .expect("spawn fanout isolate");
    let joined = join_scheduler
        .run_until_idle()
        .expect("join runtime should drain queue");
    println!("processed {joined} join items");
}
