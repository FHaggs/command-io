use std::collections::VecDeque;

use crate::{
    arena::Handle,
    effects::Wait,
    runtime::{Envelope, RuntimeError},
};

pub trait WaitBackend<M> {
    fn submit(&mut self, target: Handle, wait: Wait<M>) -> Result<(), RuntimeError>;

    fn poll(
        &mut self,
        queue: &mut VecDeque<Envelope<M>>,
        queue_capacity: usize,
    ) -> Result<bool, RuntimeError>;

    fn has_pending(&self) -> bool;
}

struct PendingWait<M> {
    target: Handle,
    wait: Wait<M>,
}

pub struct FakeWaitBackend<M> {
    pending: VecDeque<PendingWait<M>>,
    capacity: usize,
}

impl<M> FakeWaitBackend<M> {
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: VecDeque::with_capacity(capacity),
            capacity,
        }
    }
}

impl<M> WaitBackend<M> for FakeWaitBackend<M> {
    fn submit(&mut self, target: Handle, wait: Wait<M>) -> Result<(), RuntimeError> {
        if self.pending.len() == self.capacity {
            return Err(RuntimeError::WaitQueueFull);
        }

        print_wait_submission(&wait);
        self.pending.push_back(PendingWait { target, wait });
        Ok(())
    }

    fn poll(
        &mut self,
        queue: &mut VecDeque<Envelope<M>>,
        queue_capacity: usize,
    ) -> Result<bool, RuntimeError> {
        let pending_len = self.pending.len();
        let mut progressed = false;

        for _ in 0..pending_len {
            let Some(pending) = self.pending.pop_front() else {
                break;
            };

            match pending.wait {
                Wait::Accept {
                    listener,
                    completion,
                } => {
                    if queue.len() == queue_capacity {
                        self.pending.push_front(PendingWait {
                            target: pending.target,
                            wait: Wait::Accept {
                                listener,
                                completion,
                            },
                        });
                        return Err(RuntimeError::QueueFull);
                    }

                    println!("fake accept complete: listener={listener}");
                    queue.push_back(Envelope {
                        target: pending.target,
                        message: completion,
                    });
                    progressed = true;
                }
                Wait::Recv { source, completion } => {
                    if queue.len() == queue_capacity {
                        self.pending.push_front(PendingWait {
                            target: pending.target,
                            wait: Wait::Recv { source, completion },
                        });
                        return Err(RuntimeError::QueueFull);
                    }

                    println!("fake recv complete: source={source}");
                    queue.push_back(Envelope {
                        target: pending.target,
                        message: completion,
                    });
                    progressed = true;
                }
                Wait::Write { sink, completion } => {
                    if queue.len() == queue_capacity {
                        self.pending.push_front(PendingWait {
                            target: pending.target,
                            wait: Wait::Write { sink, completion },
                        });
                        return Err(RuntimeError::QueueFull);
                    }

                    println!("fake write complete: sink={sink}");
                    queue.push_back(Envelope {
                        target: pending.target,
                        message: completion,
                    });
                    progressed = true;
                }
                Wait::Timer { ticks, completion } => {
                    progressed = true;

                    if ticks <= 1 {
                        if queue.len() == queue_capacity {
                            self.pending.push_front(PendingWait {
                                target: pending.target,
                                wait: Wait::Timer { ticks, completion },
                            });
                            return Err(RuntimeError::QueueFull);
                        }

                        println!("fake timer complete");
                        queue.push_back(Envelope {
                            target: pending.target,
                            message: completion,
                        });
                    } else {
                        println!("fake timer tick: remaining={}", ticks - 1);
                        self.pending.push_back(PendingWait {
                            target: pending.target,
                            wait: Wait::Timer {
                                ticks: ticks - 1,
                                completion,
                            },
                        });
                    }
                }
            }
        }

        Ok(progressed)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

fn print_wait_submission<M>(wait: &Wait<M>) {
    match wait {
        Wait::Accept { listener, .. } => {
            println!("fake accept armed: listener={listener}");
        }
        Wait::Recv { source, .. } => {
            println!("fake recv armed: source={source}");
        }
        Wait::Write { sink, .. } => {
            println!("fake write armed: sink={sink}");
        }
        Wait::Timer { ticks, .. } => {
            println!("fake timer armed: ticks={ticks}");
        }
    }
}