#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handle {
    index: u32,
    generation: u32,
}

impl Handle {
    pub fn index(self) -> u32 {
        self.index
    }

    pub fn generation(self) -> u32 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArenaError {
    Full,
    InvalidHandle,
}

#[derive(Debug)]
struct Slot<T> {
    generation: u32,
    occupied: bool,
    pending: bool,
    object: Option<T>,
}

impl<T> Slot<T> {
    fn empty() -> Self {
        Self {
            generation: 0,
            occupied: false,
            pending: false,
            object: None,
        }
    }
}

#[derive(Debug)]
pub struct Arena<T> {
    slots: Vec<Slot<T>>,
    free_list: Vec<u32>,
    len: usize,
}

impl<T> Arena<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, Slot::empty);

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

    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn insert(&mut self, value: T) -> Result<Handle, ArenaError> {
        let index = self.free_list.pop().ok_or(ArenaError::Full)? as usize;
        let slot = &mut self.slots[index];

        debug_assert!(!slot.occupied);
        debug_assert!(slot.object.is_none());

        slot.occupied = true;
        slot.pending = false;
        slot.object = Some(value);
        self.len += 1;

        Ok(Handle {
            index: index as u32,
            generation: slot.generation,
        })
    }

    pub fn contains(&self, handle: Handle) -> bool {
        self.slot(handle)
            .map(|slot| slot.occupied && slot.generation == handle.generation)
            .unwrap_or(false)
    }

    pub fn get(&self, handle: Handle) -> Option<&T> {
        let slot = self.slot(handle)?;
        if !slot.occupied || slot.generation != handle.generation {
            return None;
        }

        slot.object.as_ref()
    }

    pub fn get_mut(&mut self, handle: Handle) -> Option<&mut T> {
        let slot = self.slot_mut(handle)?;
        if !slot.occupied || slot.generation != handle.generation {
            return None;
        }

        slot.object.as_mut()
    }

    pub fn remove(&mut self, handle: Handle) -> Result<T, ArenaError> {
        let value = {
            let slot = self.slot_mut(handle).ok_or(ArenaError::InvalidHandle)?;
            if !slot.occupied || slot.generation != handle.generation {
                return Err(ArenaError::InvalidHandle);
            }

            slot.occupied = false;
            slot.pending = false;
            slot.generation = slot.generation.wrapping_add(1);
            slot.object.take().ok_or(ArenaError::InvalidHandle)?
        };

        self.len -= 1;
        self.free_list.push(handle.index);
        Ok(value)
    }

    pub fn mark_pending(&mut self, handle: Handle) -> Result<bool, ArenaError> {
        let slot = self.slot_mut(handle).ok_or(ArenaError::InvalidHandle)?;
        if !slot.occupied || slot.generation != handle.generation {
            return Err(ArenaError::InvalidHandle);
        }

        let was_pending = slot.pending;
        slot.pending = true;
        Ok(!was_pending)
    }

    pub fn clear_pending(&mut self, handle: Handle) -> Result<(), ArenaError> {
        let slot = self.slot_mut(handle).ok_or(ArenaError::InvalidHandle)?;
        if !slot.occupied || slot.generation != handle.generation {
            return Err(ArenaError::InvalidHandle);
        }

        slot.pending = false;
        Ok(())
    }

    pub fn is_pending(&self, handle: Handle) -> Option<bool> {
        let slot = self.slot(handle)?;
        if !slot.occupied || slot.generation != handle.generation {
            return None;
        }

        Some(slot.pending)
    }

    fn slot(&self, handle: Handle) -> Option<&Slot<T>> {
        self.slots.get(handle.index as usize)
    }

    fn slot_mut(&mut self, handle: Handle) -> Option<&mut Slot<T>> {
        self.slots.get_mut(handle.index as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::{Arena, ArenaError};

    #[test]
    fn reuses_slots_with_new_generation() {
        let mut arena = Arena::with_capacity(1);

        let first = arena.insert(10).unwrap();
        assert_eq!(arena.remove(first), Ok(10));

        let second = arena.insert(20).unwrap();
        assert_ne!(first.generation(), second.generation());
        assert!(arena.get(first).is_none());
        assert_eq!(arena.get(second), Some(&20));
    }

    #[test]
    fn returns_full_when_capacity_is_exhausted() {
        let mut arena = Arena::with_capacity(1);

        arena.insert(1).unwrap();
        assert_eq!(arena.insert(2), Err(ArenaError::Full));
    }

    #[test]
    fn pending_flag_is_tracked_per_slot() {
        let mut arena = Arena::with_capacity(1);

        let handle = arena.insert(1).unwrap();
        assert_eq!(arena.mark_pending(handle), Ok(true));
        assert_eq!(arena.mark_pending(handle), Ok(false));
        assert_eq!(arena.is_pending(handle), Some(true));

        arena.clear_pending(handle).unwrap();
        assert_eq!(arena.is_pending(handle), Some(false));
    }
}