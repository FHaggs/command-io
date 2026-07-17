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
#[cfg_attr(not(test), allow(dead_code))]
pub enum IoError {
    DriverFull,
    CompletionQueueFull,
    UnknownCompletion,
    Unsupported,
    System(i32),
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

#[cfg_attr(not(test), allow(dead_code))]
pub struct FakeIoDriver {
    pending: VecDeque<PendingOperation>,
    capacity: usize,
}

#[cfg(target_os = "linux")]
#[cfg_attr(not(test), allow(dead_code))]
const CANCEL_USER_DATA: u64 = u64::MAX;

#[cfg(target_os = "linux")]
#[cfg_attr(not(test), allow(dead_code))]
pub struct IoUringDriver {
    ring: io_uring::IoUring,
    pending: Vec<PendingOperation>,
    cqe_scratch: VecDeque<(u64, i32)>,
    capacity: usize,
}

#[cfg(target_os = "linux")]
#[cfg_attr(not(test), allow(dead_code))]
impl IoUringDriver {
    pub fn new(entries: u32) -> std::io::Result<Self> {
        Ok(Self {
            ring: io_uring::IoUring::new(entries)?,
            pending: Vec::with_capacity(entries as usize),
            cqe_scratch: VecDeque::with_capacity(entries as usize),
            capacity: entries as usize,
        })
    }

    fn push_entry(&mut self, entry: io_uring::squeue::Entry) -> Result<(), IoError> {
        unsafe {
            self.ring
                .submission()
                .push(&entry)
                .map_err(|_| IoError::DriverFull)
        }
    }

    fn completion_result(pending: PendingOperation, result: i32) -> IoResult {
        if result == -libc::ECANCELED {
            return IoResult::Cancelled;
        }

        match pending.op {
            Operation::Accept(_) => IoResult::Accept(if result >= 0 {
                Ok(result)
            } else {
                Err(IoError::System(-result))
            }),
            Operation::Recv(mut op) => IoResult::Recv(if result >= 0 {
                op.buf.truncate(result as usize);
                Ok(op.buf)
            } else {
                Err(IoError::System(-result))
            }),
            Operation::Send(_) => IoResult::Send(if result >= 0 {
                Ok(result as usize)
            } else {
                Err(IoError::System(-result))
            }),
            Operation::Timer(_) => IoResult::Timer(Err(IoError::Unsupported)),
        }
    }

    fn drain_cqes(
        &mut self,
        max_completions: usize,
        completed: &mut Vec<DriverCompletion>,
    ) -> Result<bool, IoError> {
        let mut progressed = false;
        while completed.len() < max_completions && completed.len() < completed.capacity() {
            let Some((user_data, result)) = self.cqe_scratch.pop_front() else {
                break;
            };
            let completion = CompletionHandle::from_raw(user_data);
            let index = self
                .pending
                .iter()
                .position(|pending| pending.completion == completion)
                .ok_or(IoError::UnknownCompletion)?;
            let pending = self.pending.swap_remove(index);
            completed.push(DriverCompletion {
                completion,
                result: Self::completion_result(pending, result),
            });
            progressed = true;
        }
        Ok(progressed)
    }
}

#[cfg(target_os = "linux")]
impl IoDriver for IoUringDriver {
    fn can_submit(&self, additional: usize) -> bool {
        self.pending.len().saturating_add(additional) <= self.capacity
    }

    fn submit(&mut self, completion: CompletionHandle, op: Operation) -> Result<(), IoError> {
        if !self.can_submit(1) {
            return Err(IoError::DriverFull);
        }
        if matches!(op, Operation::Timer(_)) {
            return Err(IoError::Unsupported);
        }

        self.pending.push(PendingOperation {
            completion,
            op,
            cancelled: false,
        });
        let pending = self.pending.last().expect("operation was just inserted");
        let user_data = completion.into_raw();
        let entry = match &pending.op {
            Operation::Accept(op) => io_uring::opcode::Accept::new(
                io_uring::types::Fd(op.listener),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
            .build()
            .user_data(user_data),
            Operation::Recv(op) => io_uring::opcode::Recv::new(
                io_uring::types::Fd(op.fd),
                op.buf.as_ptr().cast_mut(),
                op.buf.len() as u32,
            )
            .flags(op.flags)
            .build()
            .user_data(user_data),
            Operation::Send(op) => io_uring::opcode::Send::new(
                io_uring::types::Fd(op.fd),
                op.buf.as_ptr(),
                op.buf.len() as u32,
            )
            .flags(op.flags)
            .build()
            .user_data(user_data),
            Operation::Timer(_) => unreachable!("timer was rejected before submission"),
        };

        if let Err(error) = self.push_entry(entry) {
            let _ = self.pending.pop();
            return Err(error);
        }
        Ok(())
    }

    fn cancel(&mut self, completion: CompletionHandle) -> Result<(), IoError> {
        let pending = self
            .pending
            .iter_mut()
            .find(|pending| pending.completion == completion)
            .ok_or(IoError::UnknownCompletion)?;
        if pending.cancelled {
            return Ok(());
        }
        pending.cancelled = true;

        self.push_entry(
            io_uring::opcode::AsyncCancel::new(completion.into_raw())
                .build()
                .user_data(CANCEL_USER_DATA),
        )
    }

    fn step(
        &mut self,
        max_completions: usize,
        completed: &mut Vec<DriverCompletion>,
    ) -> Result<bool, IoError> {
        if max_completions == 0 {
            return Ok(false);
        }

        let progressed = self.drain_cqes(max_completions, completed)?;
        if completed.len() == max_completions || completed.len() == completed.capacity() {
            return Ok(progressed);
        }
        if self.pending.is_empty() {
            return Ok(progressed);
        }

        self.ring
            .submit_and_wait(1)
            .map_err(|error| IoError::System(error.raw_os_error().unwrap_or(libc::EIO)))?;

        {
            let mut completion_queue = self.ring.completion();
            for cqe in &mut completion_queue {
                if cqe.user_data() == CANCEL_USER_DATA {
                    continue;
                }
                debug_assert!(self.cqe_scratch.len() < self.cqe_scratch.capacity());
                self.cqe_scratch.push_back((cqe.user_data(), cqe.result()));
            }
        }

        Ok(self.drain_cqes(max_completions, completed)? || progressed)
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }
}

#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
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

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_driver_echoes_through_a_real_tcp_socket() {
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::os::fd::AsRawFd;

        use super::{IoUringDriver, RecvOp, SendOp};

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        let (server, _) = listener.accept().unwrap();
        let mut driver = match IoUringDriver::new(8) {
            Ok(driver) => driver,
            Err(error) if matches!(error.raw_os_error(), Some(libc::EPERM | libc::ENOSYS)) => {
                return;
            }
            Err(error) => panic!("io_uring setup failed: {error}"),
        };
        let completion = CompletionHandle::test_handle(0, 0);
        let mut completed = Vec::with_capacity(1);

        client.write_all(b"ping").unwrap();
        driver
            .submit(
                completion,
                Operation::Recv(RecvOp {
                    fd: server.as_raw_fd(),
                    buf: vec![0; 16],
                    flags: 0,
                }),
            )
            .unwrap();
        assert!(driver.step(1, &mut completed).unwrap());
        let bytes = match completed.pop().unwrap().result {
            IoResult::Recv(Ok(bytes)) => bytes,
            other => panic!("recv failed: {other:?}"),
        };
        assert_eq!(bytes, b"ping");

        driver
            .submit(
                completion,
                Operation::Send(SendOp {
                    fd: server.as_raw_fd(),
                    buf: bytes,
                    flags: 0,
                }),
            )
            .unwrap();
        assert!(driver.step(1, &mut completed).unwrap());
        assert_eq!(completed.pop().unwrap().result, IoResult::Send(Ok(4)));

        let mut echoed = [0; 4];
        client.read_exact(&mut echoed).unwrap();
        assert_eq!(&echoed, b"ping");
    }
}
