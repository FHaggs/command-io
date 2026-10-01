//! Explicit, nonblocking I/O stepping on top of io_uring.
//!
//! `IoContext::submit` only queues an SQE. `IoContext::step` flushes queued
//! SQEs, retires the CQEs that are already available, marks their slots
//! `Ready`, and pushes their handles onto a bounded ready queue. Nothing here
//! advances application state; owners consume ready results on their own turn.

use std::collections::VecDeque;
use std::io;
use std::os::unix::io::AsRawFd;

use io_uring::squeue::Entry;
use io_uring::{IoUring, opcode, types::Fd};

use crate::completion::{
    CompletionArena, CompletionError, CompletionHandle, CompletionState, OwnerHandle, ReadOp,
};

/// Translates submissions to SQEs and CQEs to `(user_data, result)` pairs.
pub struct IoEngine {
    ring: IoUring,
}

impl IoEngine {
    pub fn new(entries: u32) -> io::Result<Self> {
        Ok(Self {
            ring: IoUring::new(entries)?,
        })
    }

    /// Queues an SQE without entering the kernel.
    ///
    /// # Safety
    /// Every resource referenced by `entry` must stay valid until its CQE has
    /// been retired.
    unsafe fn push(&mut self, entry: &Entry) -> Result<(), CompletionError> {
        unsafe { self.ring.submission().push(entry) }.map_err(|_| CompletionError::Full)
    }

    /// Hands queued SQEs to the kernel without waiting for completions.
    fn flush(&mut self) -> io::Result<usize> {
        self.ring.submit()
    }

    /// Blocks until at least `want` CQEs are available. Only used on teardown.
    fn wait(&mut self, want: usize) -> io::Result<usize> {
        self.ring.submit_and_wait(want)
    }

    /// Retires at most `limit` currently available CQEs.
    fn drain(&mut self, limit: usize, mut on_cqe: impl FnMut(u64, i32)) -> usize {
        let mut retired = 0;
        for cqe in self.ring.completion().take(limit) {
            on_cqe(cqe.user_data(), cqe.result());
            retired += 1;
        }
        retired
    }
}

pub struct IoContext {
    engine: IoEngine,
    completions: CompletionArena,
    ready: VecDeque<CompletionHandle>,
    ready_capacity: usize,
    in_flight: usize,
}

impl IoContext {
    /// Creates a context with `capacity` completion slots. The ring is sized
    /// so that every slot can be in flight at once without SQ/CQ overflow.
    pub fn new(capacity: u32) -> io::Result<Self> {
        let capacity = capacity.max(1);
        Ok(Self {
            engine: IoEngine::new(capacity.next_power_of_two())?,
            completions: CompletionArena::with_capacity(capacity as usize),
            ready: VecDeque::with_capacity(capacity as usize),
            ready_capacity: capacity as usize,
            in_flight: 0,
        })
    }

    pub fn acquire(&mut self, owner: OwnerHandle) -> Result<CompletionHandle, CompletionError> {
        self.completions.acquire(owner)
    }

    pub fn release(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<Option<ReadOp>, CompletionError> {
        self.completions.release(owner, completion)
    }

    pub fn prepare_read(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
        op: ReadOp,
    ) -> Result<Option<ReadOp>, CompletionError> {
        self.completions.prepare_read(owner, completion, op)
    }

    pub fn read_op(
        &self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<&ReadOp, CompletionError> {
        self.completions.read_op(owner, completion)
    }

    pub fn owner(&self, completion: CompletionHandle) -> Result<OwnerHandle, CompletionError> {
        self.completions.owner(completion)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn state(&self, completion: CompletionHandle) -> Result<CompletionState, CompletionError> {
        self.completions.state(completion)
    }

    pub fn take_result(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<io::Result<usize>, CompletionError> {
        self.completions.take_result(owner, completion)
    }

    /// Verifies ownership and `Idle`, marks the slot `Submitted`, and queues
    /// its SQE. The kernel does not see it until the next [`Self::step`].
    pub fn submit(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<(), CompletionError> {
        let op = self.completions.begin_submit(owner, completion)?;
        let len = u32::try_from(op.buf.len()).unwrap_or(u32::MAX);
        let entry = opcode::Read::new(Fd(op.file.as_raw_fd()), op.buf.as_mut_ptr(), len)
            .offset(op.offset)
            .build()
            .user_data(completion.into_raw());

        // SAFETY: the file and buffer are owned by the slot, whose heap storage
        // never moves, and the slot cannot be released or re-prepared while
        // `Submitted`. Drop waits for in-flight operations before freeing them.
        if let Err(err) = unsafe { self.engine.push(&entry) } {
            self.completions.abort_submit(completion);
            return Err(err);
        }
        self.in_flight += 1;
        Ok(())
    }

    /// Flushes queued SQEs and retires available CQEs without blocking.
    /// Returns the number of completions that became `Ready`.
    pub fn step(&mut self) -> io::Result<usize> {
        self.engine.flush()?;
        Ok(self.retire_available())
    }

    /// Pops the next ready completion. Handles whose slot is no longer
    /// `Ready` (already consumed or released) are skipped.
    pub fn pop_ready(&mut self) -> Option<CompletionHandle> {
        while let Some(handle) = self.ready.pop_front() {
            if self.completions.state(handle) == Ok(CompletionState::Ready) {
                return Some(handle);
            }
        }
        None
    }

    fn retire_available(&mut self) -> usize {
        // Never retire more CQEs than the ready queue can hold; the rest stay
        // in the CQ until the next step.
        let room = self.ready_capacity - self.ready.len();
        let Self {
            engine,
            completions,
            ready,
            in_flight,
            ..
        } = self;

        let mut became_ready = 0;
        engine.drain(room, |user_data, res| {
            let handle = CompletionHandle::from_raw(user_data);
            let result = if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else {
                Ok(res as usize)
            };
            *in_flight -= 1;
            // A stale or unexpected handle is rejected rather than delivered.
            if completions.complete(handle, result).is_ok() {
                ready.push_back(handle);
                became_ready += 1;
            }
        });
        became_ready
    }
}

impl Drop for IoContext {
    fn drop(&mut self) {
        // The kernel may still write into slot buffers; they must outlive
        // every in-flight operation.
        while self.in_flight > 0 {
            self.ready.clear();
            if self.engine.wait(1).is_err() && self.retire_available() == 0 {
                // Cannot confirm the kernel is done: leak the resources rather
                // than free memory it might still write to.
                let completions =
                    std::mem::replace(&mut self.completions, CompletionArena::with_capacity(0));
                std::mem::forget(completions);
                return;
            }
            self.retire_available();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    const A: OwnerHandle = OwnerHandle(1);
    const B: OwnerHandle = OwnerHandle(2);

    /// Steps until `count` completions are ready, bounded to avoid hanging.
    fn step_until_ready(io: &mut IoContext, count: usize) -> Vec<CompletionHandle> {
        let mut ready = Vec::new();
        for _ in 0..1_000_000 {
            io.step().unwrap();
            while let Some(handle) = io.pop_ready() {
                ready.push(handle);
            }
            if ready.len() >= count {
                return ready;
            }
        }
        panic!("completions never became ready");
    }

    #[test]
    fn cqe_result_is_stored_in_slot_selected_by_user_data() {
        let mut io = IoContext::new(4).unwrap();
        let cargo = io.acquire(A).unwrap();
        let readme = io.acquire(B).unwrap();
        io.prepare_read(
            A,
            cargo,
            ReadOp::new(File::open("Cargo.toml").unwrap(), 4096),
        )
        .unwrap();
        io.prepare_read(
            B,
            readme,
            ReadOp::new(File::open("README.md").unwrap(), 4096),
        )
        .unwrap();
        io.submit(A, cargo).unwrap();
        io.submit(B, readme).unwrap();

        let ready = step_until_ready(&mut io, 2);
        assert!(ready.contains(&cargo) && ready.contains(&readme));
        assert_eq!(io.owner(cargo), Ok(A));
        assert_eq!(io.owner(readme), Ok(B));

        let n = io.take_result(A, cargo).unwrap().unwrap();
        assert!(io.read_op(A, cargo).unwrap().buf[..n].starts_with(b"[package]"));
        let n = io.take_result(B, readme).unwrap().unwrap();
        assert!(io.read_op(B, readme).unwrap().buf[..n].starts_with(b"# command-io"));
    }

    #[test]
    fn successful_result_resets_slot_to_idle_and_can_be_reused() {
        let mut io = IoContext::new(1).unwrap();
        let handle = io.acquire(A).unwrap();
        io.prepare_read(A, handle, ReadOp::new(File::open("Cargo.toml").unwrap(), 8))
            .unwrap();

        for _ in 0..2 {
            io.submit(A, handle).unwrap();
            assert_eq!(io.state(handle), Ok(CompletionState::Submitted));
            step_until_ready(&mut io, 1);
            assert_eq!(io.take_result(A, handle).unwrap().unwrap(), 8);
            assert_eq!(io.state(handle), Ok(CompletionState::Idle));
        }
    }

    #[test]
    fn negative_cqe_becomes_ready_error_for_same_owner() {
        let mut io = IoContext::new(1).unwrap();
        let handle = io.acquire(A).unwrap();
        // Reading a directory fails with EISDIR.
        io.prepare_read(A, handle, ReadOp::new(File::open("src").unwrap(), 64))
            .unwrap();
        io.submit(A, handle).unwrap();

        let ready = step_until_ready(&mut io, 1);
        assert_eq!(ready, vec![handle]);
        assert_eq!(io.owner(handle), Ok(A));
        let err = io.take_result(A, handle).unwrap().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EISDIR));
    }

    #[test]
    fn drop_with_operation_in_flight_waits_for_kernel() {
        let mut io = IoContext::new(1).unwrap();
        let handle = io.acquire(A).unwrap();
        io.prepare_read(
            A,
            handle,
            ReadOp::new(File::open("Cargo.toml").unwrap(), 4096),
        )
        .unwrap();
        io.submit(A, handle).unwrap();
        drop(io);
    }
}
