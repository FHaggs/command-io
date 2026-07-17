use crate::arena::Handle;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompletionHandle {
    index: u32,
    generation: u32,
}

impl CompletionHandle {
    pub const INVALID: Self = Self {
        index: u32::MAX,
        generation: u32::MAX,
    };

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn into_raw(self) -> u64 {
        (u64::from(self.generation) << 32) | u64::from(self.index)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn from_raw(raw: u64) -> Self {
        Self {
            index: raw as u32,
            generation: (raw >> 32) as u32,
        }
    }

    #[cfg(test)]
    pub(crate) const fn test_handle(index: u32, generation: u32) -> Self {
        Self { index, generation }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionState {
    Idle,
    Submitted,
    Cancelling,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionError {
    Full,
    InvalidHandle,
    WrongOwner,
    InvalidState,
}

#[derive(Debug)]
struct Slot<R> {
    generation: u32,
    owner: Option<Handle>,
    state: Option<CompletionState>,
    result: Option<R>,
}

impl<R> Slot<R> {
    fn free() -> Self {
        Self {
            generation: 0,
            owner: None,
            state: None,
            result: None,
        }
    }

    fn is_free(&self) -> bool {
        self.owner.is_none()
    }
}

#[derive(Debug)]
pub struct CompletionArena<R> {
    slots: Vec<Slot<R>>,
    free_list: Vec<u32>,
    len: usize,
}

impl<R> CompletionArena<R> {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, Slot::free);

        let mut free_list = Vec::with_capacity(capacity);
        for index in (0..capacity).rev() {
            free_list.push(index as u32);
        }

        Self {
            slots,
            free_list,
            len: 0,
        }
    }

    pub fn acquire(&mut self, owner: Handle) -> Result<CompletionHandle, CompletionError> {
        let index = self.free_list.pop().ok_or(CompletionError::Full)? as usize;
        let slot = &mut self.slots[index];

        debug_assert!(slot.is_free());
        slot.owner = Some(owner);
        slot.state = Some(CompletionState::Idle);
        self.len += 1;

        Ok(CompletionHandle {
            index: index as u32,
            generation: slot.generation,
        })
    }

    pub fn owner(&self, completion: CompletionHandle) -> Result<Handle, CompletionError> {
        self.slot(completion)?
            .owner
            .ok_or(CompletionError::InvalidHandle)
    }

    pub fn state(&self, completion: CompletionHandle) -> Result<CompletionState, CompletionError> {
        self.slot(completion)?
            .state
            .ok_or(CompletionError::InvalidHandle)
    }

    pub fn submit(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
    ) -> Result<(), CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if slot.state != Some(CompletionState::Idle) {
            return Err(CompletionError::InvalidState);
        }

        slot.state = Some(CompletionState::Submitted);
        Ok(())
    }

    pub fn unsubmit(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
    ) -> Result<(), CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if slot.state != Some(CompletionState::Submitted) {
            return Err(CompletionError::InvalidState);
        }

        slot.state = Some(CompletionState::Idle);
        Ok(())
    }

    pub fn begin_cancel(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
    ) -> Result<CompletionState, CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        match slot.state.ok_or(CompletionError::InvalidHandle)? {
            CompletionState::Idle | CompletionState::Ready => Ok(slot.state.unwrap()),
            CompletionState::Submitted => {
                slot.state = Some(CompletionState::Cancelling);
                Ok(CompletionState::Cancelling)
            }
            CompletionState::Cancelling => Err(CompletionError::InvalidState),
        }
    }

    pub fn complete(
        &mut self,
        completion: CompletionHandle,
        result: R,
    ) -> Result<(), CompletionError> {
        let slot = self.slot_mut(completion)?;
        match slot.state.ok_or(CompletionError::InvalidHandle)? {
            CompletionState::Submitted | CompletionState::Cancelling => {
                slot.state = Some(CompletionState::Ready);
                slot.result = Some(result);
                Ok(())
            }
            CompletionState::Idle | CompletionState::Ready => Err(CompletionError::InvalidState),
        }
    }

    pub fn take_result(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
    ) -> Result<R, CompletionError> {
        let slot = self.owned_slot_mut(owner, completion)?;
        if slot.state != Some(CompletionState::Ready) {
            return Err(CompletionError::InvalidState);
        }

        slot.state = Some(CompletionState::Idle);
        slot.result.take().ok_or(CompletionError::InvalidState)
    }

    pub fn release(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
    ) -> Result<(), CompletionError> {
        let index = completion.index as usize;
        let slot = self.owned_slot_mut(owner, completion)?;
        if !matches!(
            slot.state,
            Some(CompletionState::Idle | CompletionState::Ready)
        ) {
            return Err(CompletionError::InvalidState);
        }

        slot.owner = None;
        slot.state = None;
        slot.result = None;
        slot.generation = slot.generation.wrapping_add(1);
        self.len -= 1;
        self.free_list.push(index as u32);
        Ok(())
    }

    pub fn has_owner(&self, owner: Handle) -> bool {
        self.slots.iter().any(|slot| slot.owner == Some(owner))
    }

    fn slot(&self, completion: CompletionHandle) -> Result<&Slot<R>, CompletionError> {
        let slot = self
            .slots
            .get(completion.index as usize)
            .ok_or(CompletionError::InvalidHandle)?;
        if slot.generation != completion.generation || slot.is_free() {
            return Err(CompletionError::InvalidHandle);
        }

        Ok(slot)
    }

    fn slot_mut(&mut self, completion: CompletionHandle) -> Result<&mut Slot<R>, CompletionError> {
        let slot = self
            .slots
            .get_mut(completion.index as usize)
            .ok_or(CompletionError::InvalidHandle)?;
        if slot.generation != completion.generation || slot.is_free() {
            return Err(CompletionError::InvalidHandle);
        }

        Ok(slot)
    }

    fn owned_slot_mut(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
    ) -> Result<&mut Slot<R>, CompletionError> {
        let slot = self.slot_mut(completion)?;
        if slot.owner != Some(owner) {
            return Err(CompletionError::WrongOwner);
        }

        Ok(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::{CompletionArena, CompletionError, CompletionState};
    use crate::arena::Arena;

    fn owner() -> crate::arena::Handle {
        let mut isolates = Arena::with_capacity(2);
        isolates.insert(()).unwrap()
    }

    #[test]
    fn completion_is_reused_with_a_new_generation_after_release() {
        let owner = owner();
        let mut completions = CompletionArena::<u8>::with_capacity(1);

        let first = completions.acquire(owner).unwrap();
        completions.release(owner, first).unwrap();
        let second = completions.acquire(owner).unwrap();

        assert_eq!(first.index, second.index);
        assert_eq!(
            completions.state(first),
            Err(CompletionError::InvalidHandle)
        );
        assert_eq!(completions.state(second), Ok(CompletionState::Idle));
    }

    #[test]
    fn submitted_completion_cannot_be_released_until_reaped() {
        let owner = owner();
        let mut completions = CompletionArena::with_capacity(1);
        let completion = completions.acquire(owner).unwrap();

        completions.submit(owner, completion).unwrap();
        assert_eq!(
            completions.release(owner, completion),
            Err(CompletionError::InvalidState)
        );

        completions.complete(completion, 7).unwrap();
        assert_eq!(completions.take_result(owner, completion), Ok(7));
        completions.release(owner, completion).unwrap();
    }

    #[test]
    fn completion_operations_require_the_owning_isolate() {
        let mut isolates = Arena::with_capacity(2);
        let left = isolates.insert(()).unwrap();
        let right = isolates.insert(()).unwrap();
        let mut completions = CompletionArena::<()>::with_capacity(1);
        let completion = completions.acquire(left).unwrap();

        assert_eq!(
            completions.submit(right, completion),
            Err(CompletionError::WrongOwner)
        );
    }
}
