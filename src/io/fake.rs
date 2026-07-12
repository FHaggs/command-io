use std::collections::VecDeque;

use crate::effects::{OpToken, Wait};
use crate::runtime::RuntimeError;

use super::backend::WaitBackend;

/// An in-process backend that completes every armed operation deterministically.
///
/// Accept/Recv/Write fire on the next poll. Timers count down one tick per poll
/// and fire when they reach zero. Nothing about isolates or messages lives here:
/// the backend only owns tokens and their resource descriptors.
pub struct FakeWaitBackend {
    pending: VecDeque<(OpToken, Wait)>,
    capacity: usize,
}

impl FakeWaitBackend {
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: VecDeque::with_capacity(capacity),
            capacity,
        }
    }
}

impl WaitBackend for FakeWaitBackend {
    fn submit(&mut self, token: OpToken, wait: Wait) -> Result<(), RuntimeError> {
        if self.pending.len() == self.capacity {
            return Err(RuntimeError::WaitQueueFull);
        }

        print_wait_submission(token, &wait);
        self.pending.push_back((token, wait));
        Ok(())
    }

    fn cancel(&mut self, token: OpToken) {
        if let Some(pos) = self.pending.iter().position(|(t, _)| *t == token) {
            self.pending.remove(pos);
            let OpToken(id) = token;
            println!("fake cancel: token={id}");
        }
    }

    fn poll(&mut self, ready: &mut Vec<OpToken>) -> Result<bool, RuntimeError> {
        let pending_len = self.pending.len();
        let mut progressed = false;

        for _ in 0..pending_len {
            let Some((token, wait)) = self.pending.pop_front() else {
                break;
            };

            let OpToken(id) = token;
            match wait {
                Wait::Accept { listener } => {
                    println!("fake accept complete: token={id} listener={listener}");
                    ready.push(token);
                    progressed = true;
                }
                Wait::Recv { source } => {
                    println!("fake recv complete: token={id} source={source}");
                    ready.push(token);
                    progressed = true;
                }
                Wait::Write { sink } => {
                    println!("fake write complete: token={id} sink={sink}");
                    ready.push(token);
                    progressed = true;
                }
                Wait::Timer { ticks } => {
                    progressed = true;
                    if ticks <= 1 {
                        println!("fake timer complete: token={id}");
                        ready.push(token);
                    } else {
                        println!("fake timer tick: token={id} remaining={}", ticks - 1);
                        self.pending
                            .push_back((token, Wait::Timer { ticks: ticks - 1 }));
                    }
                }
            }
        }

        Ok(progressed)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn can_submit(&self, additional: usize) -> bool {
        self.pending.len() + additional <= self.capacity
    }
}

fn print_wait_submission(token: OpToken, wait: &Wait) {
    let OpToken(id) = token;
    match wait {
        Wait::Accept { listener } => {
            println!("fake accept armed: token={id} listener={listener}");
        }
        Wait::Recv { source } => {
            println!("fake recv armed: token={id} source={source}");
        }
        Wait::Write { sink } => {
            println!("fake write armed: token={id} sink={sink}");
        }
        Wait::Timer { ticks } => {
            println!("fake timer armed: token={id} ticks={ticks}");
        }
    }
}
