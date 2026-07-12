mod arena;
mod effects;
mod io;
mod runtime;

use effects::{RuntimeMessage, TurnEffects};
use runtime::{Isolate, RuntimeError, Scheduler};

#[derive(Debug)]
struct DemoIsolate {
    remaining_ticks: u8,
}

#[derive(Clone, Copy, Debug)]
enum DemoMessage {
    Tick,
    TearDown,
}

impl Isolate for DemoIsolate {
    type Message = DemoMessage;

    fn handle(
        &mut self,
        msg: RuntimeMessage<Self::Message>,
        effects: &mut TurnEffects<Self::Message>,
    ) -> Result<(), RuntimeError> {
        match msg {
            RuntimeMessage::Init => {
                println!("init: scheduling first tick: step by 2");
                let _ = effects.send_self(DemoMessage::Tick);
                effects.send_self(DemoMessage::Tick)?;
            }
            RuntimeMessage::User(DemoMessage::Tick) => {
                if self.remaining_ticks == 0 {
                    return Ok(());
                }
                self.remaining_ticks -= 1;
                println!("tick: remaining={}", self.remaining_ticks);

                if self.remaining_ticks == 0 {
                    effects.wait_accept(99, DemoMessage::TearDown)?;
                } else {
                    effects.send_self(DemoMessage::Tick)?;
                }
            }
            RuntimeMessage::User(DemoMessage::TearDown) => {
                println!("tear down: shutting down");
                effects.destroy_self()?;
            }
        }

        Ok(())
    }
}

fn main() {
    let mut scheduler = Scheduler::new(16, 64, 8);
    let isolate = DemoIsolate {
        remaining_ticks: 5,
    };

    let _handle = scheduler.spawn(isolate).expect("spawn demo isolate");

    let processed = scheduler.run_until_idle().expect("runtime should drain queue");
    println!("processed {processed} work items");
}
