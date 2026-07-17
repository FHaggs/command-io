use std::collections::VecDeque;
use std::os::fd::RawFd;

use crate::completion::CompletionHandle;

pub type RawFdLike = RawFd;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptOp {
    pub listener: RawFdLike,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecvOp {
    pub fd: RawFdLike,
    pub buf: Vec<u8>,
    pub flags: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendOp {
    pub fd: RawFdLike,
    pub buf: Vec<u8>,
    pub flags: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerOp {
    pub ticks: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub enum Operation {
    Accept(AcceptOp),
    Recv(RecvOp),
    Send(SendOp),
    Timer(TimerOp),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoError {
    DriverFull,
    CompletionQueueFull,
    UnknownCompletion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IoResult {
    Accept(Result<RawFdLike, IoError>),
    Recv(Result<Vec<u8>, IoError>),
    Send(Result<usize, IoError>),
    Timer(Result<(), IoError>),
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriverCompletion {
    pub completion: CompletionHandle,
    pub result: IoResult,
}

pub trait IoDriver {
    fn can_submit(&self, additional: usize) -> bool;

    fn submit(&mut self, completion: CompletionHandle, op: Operation) -> Result<(), IoError>;

    fn cancel(&mut self, completion: CompletionHandle) -> Result<(), IoError>;

    fn step(
        &mut self,
        max_completions: usize,
        completed: &mut Vec<DriverCompletion>,
    ) -> Result<bool, IoError>;

    fn has_pending(&self) -> bool;
}

struct PendingOperation {
    completion: CompletionHandle,
    op: Operation,
    cancelled: bool,
}

pub struct FakeIoDriver {
    pending: VecDeque<PendingOperation>,
    capacity: usize,
}

impl FakeIoDriver {
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: VecDeque::with_capacity(capacity),
            capacity,
        }
    }
}

impl IoDriver for FakeIoDriver {
    fn can_submit(&self, additional: usize) -> bool {
        self.pending.len().saturating_add(additional) <= self.capacity
    }

    fn submit(&mut self, completion: CompletionHandle, op: Operation) -> Result<(), IoError> {
        if !self.can_submit(1) {
            return Err(IoError::DriverFull);
        }

        self.pending.push_back(PendingOperation {
            completion,
            op,
            cancelled: false,
        });
        Ok(())
    }

    fn cancel(&mut self, completion: CompletionHandle) -> Result<(), IoError> {
        let pending = self
            .pending
            .iter_mut()
            .find(|pending| pending.completion == completion)
            .ok_or(IoError::UnknownCompletion)?;
        pending.cancelled = true;
        Ok(())
    }

    fn step(
        &mut self,
        max_completions: usize,
        completed: &mut Vec<DriverCompletion>,
    ) -> Result<bool, IoError> {
        let pending_len = self.pending.len().min(max_completions);
        let mut progressed = false;

        for _ in 0..pending_len {
            if completed.len() == completed.capacity() {
                return Err(IoError::CompletionQueueFull);
            }

            let pending = self
                .pending
                .pop_front()
                .expect("pending length was measured");
            let result = if pending.cancelled {
                IoResult::Cancelled
            } else {
                complete_fake_operation(pending.op)
            };
            completed.push(DriverCompletion {
                completion: pending.completion,
                result,
            });
            progressed = true;
        }

        Ok(progressed)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

fn complete_fake_operation(op: Operation) -> IoResult {
    match op {
        Operation::Accept(AcceptOp { listener }) => IoResult::Accept(Ok(listener)),
        Operation::Recv(RecvOp { .. }) => IoResult::Recv(Ok(Vec::new())),
        Operation::Send(SendOp { buf, .. }) => IoResult::Send(Ok(buf.len())),
        Operation::Timer(TimerOp { .. }) => IoResult::Timer(Ok(())),
    }
}

#[cfg(test)]
mod tests {
    use super::{FakeIoDriver, IoDriver, IoResult, Operation, TimerOp};
    use crate::completion::CompletionHandle;

    #[test]
    fn fake_driver_reaps_submitted_operation_by_completion() {
        let mut driver = FakeIoDriver::new(1);
        let completion = CompletionHandle::INVALID;
        let mut completed = Vec::with_capacity(1);

        driver
            .submit(completion, Operation::Timer(TimerOp { ticks: 1 }))
            .unwrap();
        assert!(driver.step(1, &mut completed).unwrap());
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].completion, completion);
        assert_eq!(completed[0].result, IoResult::Timer(Ok(())));
    }
}
