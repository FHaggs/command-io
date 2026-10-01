//! A minimal owner state machine: read one chunk of a file.

use std::fs::File;
use std::io;

use crate::completion::{CompletionError, CompletionHandle, OwnerHandle, ReadOp};
use crate::io::IoContext;

#[derive(Debug)]
pub enum ReaderState {
    Start,
    Reading { completion: CompletionHandle },
    Done { bytes: usize },
    Failed(io::Error),
}

pub struct Reader {
    owner: OwnerHandle,
    completion: CompletionHandle,
    state: ReaderState,
}

impl Reader {
    /// Acquires the reader's completion slot once and installs the read.
    pub fn new(
        owner: OwnerHandle,
        io: &mut IoContext,
        file: File,
        buf_len: usize,
    ) -> Result<Self, CompletionError> {
        let completion = io.acquire(owner)?;
        io.prepare_read(owner, completion, ReadOp::new(file, buf_len))?;
        Ok(Self {
            owner,
            completion,
            state: ReaderState::Start,
        })
    }

    pub fn state(&self) -> &ReaderState {
        &self.state
    }

    pub fn is_finished(&self) -> bool {
        matches!(
            self.state,
            ReaderState::Done { .. } | ReaderState::Failed(_)
        )
    }

    /// One turn: submits in `Start`, otherwise does nothing.
    pub fn step(&mut self, io: &mut IoContext) -> Result<(), CompletionError> {
        if let ReaderState::Start = self.state {
            io.submit(self.owner, self.completion)?;
            self.state = ReaderState::Reading {
                completion: self.completion,
            };
        }
        Ok(())
    }

    /// Consumes a routed completion if it is the one this reader is waiting on.
    pub fn on_io_completed(
        &mut self,
        io: &mut IoContext,
        completion: CompletionHandle,
    ) -> Result<(), CompletionError> {
        match self.state {
            ReaderState::Reading {
                completion: expected,
            } if expected == completion => {}
            _ => return Err(CompletionError::InvalidState),
        }
        self.state = match io.take_result(self.owner, completion)? {
            Ok(bytes) => ReaderState::Done { bytes },
            Err(err) => ReaderState::Failed(err),
        };
        Ok(())
    }

    /// The bytes read, once `Done`.
    pub fn data<'io>(&self, io: &'io IoContext) -> Option<&'io [u8]> {
        let ReaderState::Done { bytes } = self.state else {
            return None;
        };
        let op = io.read_op(self.owner, self.completion).ok()?;
        Some(&op.buf[..bytes])
    }

    /// Returns the completion slot to the arena.
    pub fn close(self, io: &mut IoContext) -> Result<(), CompletionError> {
        io.release(self.owner, self.completion).map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::completion::CompletionState;

    const OWNER: OwnerHandle = OwnerHandle(0);

    #[test]
    fn reader_stays_reading_until_io_step_retires_cqe() {
        let mut io = IoContext::new(1).unwrap();
        let mut reader =
            Reader::new(OWNER, &mut io, File::open("Cargo.toml").unwrap(), 64).unwrap();

        reader.step(&mut io).unwrap();
        let ReaderState::Reading { completion } = *reader.state() else {
            panic!("expected Reading");
        };
        // Without io.step() the SQE has not even reached the kernel.
        for _ in 0..10 {
            reader.step(&mut io).unwrap();
            assert!(matches!(reader.state(), ReaderState::Reading { .. }));
            assert_eq!(io.pop_ready(), None);
        }
        assert_eq!(io.state(completion), Ok(CompletionState::Submitted));

        let mut routed = None;
        for _ in 0..1_000_000 {
            io.step().unwrap();
            // Completion alone never advances the reader.
            assert!(matches!(reader.state(), ReaderState::Reading { .. }));
            if let Some(handle) = io.pop_ready() {
                routed = Some(handle);
                break;
            }
        }
        let handle = routed.expect("read never completed");
        assert_eq!(io.owner(handle), Ok(OWNER));

        reader.on_io_completed(&mut io, handle).unwrap();
        assert!(matches!(reader.state(), ReaderState::Done { bytes: 64 }));
        assert!(reader.data(&io).unwrap().starts_with(b"[package]"));
        assert_eq!(io.state(completion), Ok(CompletionState::Idle));

        reader.close(&mut io).unwrap();
        assert_eq!(io.state(completion), Err(CompletionError::InvalidHandle));
    }

    #[test]
    fn reader_rejects_unexpected_completion() {
        let mut io = IoContext::new(2).unwrap();
        let mut reader =
            Reader::new(OWNER, &mut io, File::open("Cargo.toml").unwrap(), 64).unwrap();
        let other = io.acquire(OwnerHandle(9)).unwrap();

        assert_eq!(
            reader.on_io_completed(&mut io, other),
            Err(CompletionError::InvalidState)
        );
    }

    #[test]
    fn reader_creation_fails_when_arena_is_full() {
        let mut io = IoContext::new(1).unwrap();
        let _first = Reader::new(OWNER, &mut io, File::open("Cargo.toml").unwrap(), 8).unwrap();
        let second = Reader::new(
            OwnerHandle(1),
            &mut io,
            File::open("Cargo.toml").unwrap(),
            8,
        );
        assert_eq!(second.err(), Some(CompletionError::Full));
    }
}
