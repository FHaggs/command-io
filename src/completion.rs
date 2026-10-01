//! Fixed-capacity completion slots addressed by generational handles.
//!
//! A slot is the persistent mailbox for exactly one operation at a time:
//! `Idle -> Submitted -> Ready -> Idle`. It physically owns every resource the
//! kernel may touch while the operation is in flight (file and buffer), and it
//! records the logical owner that is allowed to drive it.

use std::fmt;
use std::fs::File;
use std::io;

/// Identifies the application state machine that acquired a slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OwnerHandle(pub u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CompletionHandle {
    index: u32,
    generation: u32,
}

impl CompletionHandle {
    /// Encodes the handle for `Entry::user_data`: `(generation << 32) | index`.
    pub fn into_raw(self) -> u64 {
        ((self.generation as u64) << 32) | self.index as u64
    }

    pub fn from_raw(raw: u64) -> Self {
        Self {
            index: raw as u32,
            generation: (raw >> 32) as u32,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionState {
    Idle,
    Submitted,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionError {
    Full,
    InvalidHandle,
    WrongOwner,
    InvalidState,
    NoOperation,
}

impl fmt::Display for CompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::Full => "capacity exhausted",
            Self::InvalidHandle => "invalid or stale completion handle",
            Self::WrongOwner => "completion is owned by another owner",
            Self::InvalidState => "completion is in the wrong state for this operation",
            Self::NoOperation => "completion has no prepared operation",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for CompletionError {}

/// A semantic read operation and the resources it needs. The kernel-specific
/// SQE is built from this only at submission time.
#[derive(Debug)]
pub struct ReadOp {
    pub file: File,
    pub buf: Vec<u8>,
    /// File offset; `u64::MAX` means "use and advance the current position".
    pub offset: u64,
}

impl ReadOp {
    pub fn new(file: File, buf_len: usize) -> Self {
        Self {
            file,
            buf: vec![0; buf_len],
            offset: 0,
        }
    }
}

#[derive(Debug)]
enum Lifecycle {
    Idle,
    Submitted,
    Ready(io::Result<usize>),
}

#[derive(Debug)]
struct Slot {
    generation: u32,
    owner: Option<OwnerHandle>,
    lifecycle: Lifecycle,
    op: Option<ReadOp>,
}

impl Slot {
    fn free() -> Self {
        Self {
            generation: 0,
            owner: None,
            lifecycle: Lifecycle::Idle,
            op: None,
        }
    }

    fn state(&self) -> CompletionState {
        match self.lifecycle {
            Lifecycle::Idle => CompletionState::Idle,
            Lifecycle::Submitted => CompletionState::Submitted,
            Lifecycle::Ready(_) => CompletionState::Ready,
        }
    }
}

#[derive(Debug)]
pub struct CompletionArena {
    slots: Vec<Slot>,
    free_list: Vec<u32>,
}

impl CompletionArena {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, Slot::free);
        let free_list = (0..capacity as u32).rev().collect();
        Self { slots, free_list }
    }

    pub fn acquire(&mut self, owner: OwnerHandle) -> Result<CompletionHandle, CompletionError> {
        let index = self.free_list.pop().ok_or(CompletionError::Full)?;
        let slot = &mut self.slots[index as usize];
        debug_assert!(slot.owner.is_none());

        slot.owner = Some(owner);
        slot.lifecycle = Lifecycle::Idle;
        Ok(CompletionHandle {
            index,
            generation: slot.generation,
        })
    }

    /// Returns the slot to the free list and hands back its resources. The
    /// generation changes so outstanding copies of the handle become stale.
    pub fn release(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<Option<ReadOp>, CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if matches!(slot.lifecycle, Lifecycle::Submitted) {
            return Err(CompletionError::InvalidState);
        }

        slot.owner = None;
        slot.lifecycle = Lifecycle::Idle;
        slot.generation = slot.generation.wrapping_add(1);
        let op = slot.op.take();
        self.free_list.push(completion.index);
        Ok(op)
    }

    pub fn owner(&self, completion: CompletionHandle) -> Result<OwnerHandle, CompletionError> {
        self.slot(completion)?
            .owner
            .ok_or(CompletionError::InvalidHandle)
    }

    pub fn state(&self, completion: CompletionHandle) -> Result<CompletionState, CompletionError> {
        Ok(self.slot(completion)?.state())
    }

    /// Installs the operation to run on the next submit, returning any
    /// previous one. Only allowed while `Idle`.
    pub fn prepare_read(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
        op: ReadOp,
    ) -> Result<Option<ReadOp>, CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if !matches!(slot.lifecycle, Lifecycle::Idle) {
            return Err(CompletionError::InvalidState);
        }
        Ok(slot.op.replace(op))
    }

    /// Borrows the operation's resources. Refused while the kernel may still
    /// be writing into them.
    pub fn read_op(
        &self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<&ReadOp, CompletionError> {
        let slot = self.slot(completion)?;
        if slot.owner != Some(owner) {
            return Err(CompletionError::WrongOwner);
        }
        if matches!(slot.lifecycle, Lifecycle::Submitted) {
            return Err(CompletionError::InvalidState);
        }
        slot.op.as_ref().ok_or(CompletionError::NoOperation)
    }

    /// Moves `Idle -> Submitted` and exposes the resources needed to build the
    /// SQE. Pair with [`Self::abort_submit`] if the SQE cannot be queued.
    pub(crate) fn begin_submit(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<&mut ReadOp, CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if !matches!(slot.lifecycle, Lifecycle::Idle) {
            return Err(CompletionError::InvalidState);
        }
        let op = slot.op.as_mut().ok_or(CompletionError::NoOperation)?;
        slot.lifecycle = Lifecycle::Submitted;
        Ok(op)
    }

    pub(crate) fn abort_submit(&mut self, completion: CompletionHandle) {
        if let Ok(slot) = self.slot_mut(completion) {
            debug_assert!(matches!(slot.lifecycle, Lifecycle::Submitted));
            slot.lifecycle = Lifecycle::Idle;
        }
    }

    /// Engine-side: stores the terminal result of a submitted slot.
    pub(crate) fn complete(
        &mut self,
        completion: CompletionHandle,
        result: io::Result<usize>,
    ) -> Result<(), CompletionError> {
        let slot = self.slot_mut(completion)?;
        if !matches!(slot.lifecycle, Lifecycle::Submitted) {
            return Err(CompletionError::InvalidState);
        }
        slot.lifecycle = Lifecycle::Ready(result);
        Ok(())
    }

    /// Owner-side: consumes the terminal result and resets the slot to `Idle`.
    pub fn take_result(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<io::Result<usize>, CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if !matches!(slot.lifecycle, Lifecycle::Ready(_)) {
            return Err(CompletionError::InvalidState);
        }
        match std::mem::replace(&mut slot.lifecycle, Lifecycle::Idle) {
            Lifecycle::Ready(result) => Ok(result),
            _ => unreachable!(),
        }
    }

    fn slot(&self, completion: CompletionHandle) -> Result<&Slot, CompletionError> {
        self.slots
            .get(completion.index as usize)
            .filter(|slot| slot.owner.is_some() && slot.generation == completion.generation)
            .ok_or(CompletionError::InvalidHandle)
    }

    fn slot_mut(&mut self, completion: CompletionHandle) -> Result<&mut Slot, CompletionError> {
        self.slots
            .get_mut(completion.index as usize)
            .filter(|slot| slot.owner.is_some() && slot.generation == completion.generation)
            .ok_or(CompletionError::InvalidHandle)
    }

    fn owned_slot_mut(
        &mut self,
        owner: OwnerHandle,
        completion: CompletionHandle,
    ) -> Result<&mut Slot, CompletionError> {
        let slot = self.slot_mut(completion)?;
        if slot.owner != Some(owner) {
            return Err(CompletionError::WrongOwner);
        }
        Ok(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: OwnerHandle = OwnerHandle(1);
    const B: OwnerHandle = OwnerHandle(2);

    fn read_op() -> ReadOp {
        ReadOp::new(File::open("Cargo.toml").unwrap(), 16)
    }

    #[test]
    fn handle_round_trips_through_u64() {
        let handle = CompletionHandle {
            index: 7,
            generation: 0xDEAD_BEEF,
        };
        assert_eq!(handle.into_raw(), 0xDEAD_BEEF_0000_0007);
        assert_eq!(CompletionHandle::from_raw(handle.into_raw()), handle);
    }

    #[test]
    fn release_and_reacquire_changes_generation() {
        let mut arena = CompletionArena::with_capacity(1);
        let first = arena.acquire(A).unwrap();
        arena.release(A, first).unwrap();

        let second = arena.acquire(A).unwrap();
        assert_eq!(first.index, second.index);
        assert_ne!(first, second);
        assert_eq!(arena.state(first), Err(CompletionError::InvalidHandle));
        assert_eq!(arena.state(second), Ok(CompletionState::Idle));
    }

    #[test]
    fn wrong_owner_cannot_submit_or_take_result() {
        let mut arena = CompletionArena::with_capacity(1);
        let handle = arena.acquire(A).unwrap();
        arena.prepare_read(A, handle, read_op()).unwrap();

        assert_eq!(
            arena.begin_submit(B, handle).err(),
            Some(CompletionError::WrongOwner)
        );
        arena.begin_submit(A, handle).unwrap();
        arena.complete(handle, Ok(3)).unwrap();
        assert_eq!(
            arena.take_result(B, handle).err(),
            Some(CompletionError::WrongOwner)
        );
        assert_eq!(arena.take_result(A, handle).unwrap().unwrap(), 3);
    }

    #[test]
    fn submitted_slot_cannot_be_resubmitted_or_released() {
        let mut arena = CompletionArena::with_capacity(1);
        let handle = arena.acquire(A).unwrap();
        arena.prepare_read(A, handle, read_op()).unwrap();
        arena.begin_submit(A, handle).unwrap();

        assert_eq!(
            arena.begin_submit(A, handle).err(),
            Some(CompletionError::InvalidState)
        );
        assert_eq!(
            arena.release(A, handle).err(),
            Some(CompletionError::InvalidState)
        );
        assert_eq!(
            arena.read_op(A, handle).err(),
            Some(CompletionError::InvalidState)
        );
    }

    #[test]
    fn submit_without_operation_is_rejected() {
        let mut arena = CompletionArena::with_capacity(1);
        let handle = arena.acquire(A).unwrap();
        assert_eq!(
            arena.begin_submit(A, handle).err(),
            Some(CompletionError::NoOperation)
        );
        assert_eq!(arena.state(handle), Ok(CompletionState::Idle));
    }

    #[test]
    fn arena_exhaustion_returns_full() {
        let mut arena = CompletionArena::with_capacity(1);
        arena.acquire(A).unwrap();
        assert_eq!(arena.acquire(A), Err(CompletionError::Full));
    }
}
