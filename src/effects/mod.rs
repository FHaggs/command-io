use crate::arena::Handle;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeMessage<M> {
    Init,
    User(M),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action<M> {
    Send {
        target: Handle,
        message: RuntimeMessage<M>,
    },
    SendSelf {
        message: M,
    },
    DestroySelf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Wait<M> {
    Accept {
        listener: u32,
        completion: RuntimeMessage<M>,
    },
    Recv {
        source: u32,
        completion: RuntimeMessage<M>,
    },
    Write {
        sink: u32,
        completion: RuntimeMessage<M>,
    },
    Timer {
        ticks: u32,
        completion: RuntimeMessage<M>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectsError {
    ActionsFull,
    TurnSealed,
    WaitAlreadySet,
}

pub struct TurnEffects<M> {
    actions: Vec<Action<M>>,
    max_actions: usize,
    wait: Option<Wait<M>>,
    sealed: bool,
}

impl<M> TurnEffects<M> {
    pub fn with_capacity(max_actions: usize) -> Self {
        Self {
            actions: Vec::with_capacity(max_actions),
            max_actions,
            wait: None,
            sealed: false,
        }
    }

    pub fn reset(&mut self) {
        self.actions.clear();
        self.wait = None;
        self.sealed = false;
    }

    pub fn send(&mut self, target: Handle, message: M) -> Result<(), EffectsError> {
        self.push_action(Action::Send {
            target,
            message: RuntimeMessage::User(message),
        })
    }

    pub fn send_self(&mut self, message: M) -> Result<(), EffectsError> {
        self.push_action(Action::SendSelf { message })
    }

    pub fn destroy_self(&mut self) -> Result<(), EffectsError> {
        self.push_action(Action::DestroySelf)
    }

    pub fn wait_accept(&mut self, listener: u32, completion: M) -> Result<(), EffectsError> {
        self.set_wait(Wait::Accept {
            listener,
            completion: RuntimeMessage::User(completion),
        })
    }

    pub fn wait_recv(&mut self, source: u32, completion: M) -> Result<(), EffectsError> {
        self.set_wait(Wait::Recv {
            source,
            completion: RuntimeMessage::User(completion),
        })
    }

    pub fn wait_write(&mut self, sink: u32, completion: M) -> Result<(), EffectsError> {
        self.set_wait(Wait::Write {
            sink,
            completion: RuntimeMessage::User(completion),
        })
    }

    pub fn wait_timer(&mut self, ticks: u32, completion: M) -> Result<(), EffectsError> {
        self.set_wait(Wait::Timer {
            ticks,
            completion: RuntimeMessage::User(completion),
        })
    }

    pub fn swap_actions(&mut self, scratch: &mut Vec<Action<M>>) {
        scratch.clear();
        std::mem::swap(&mut self.actions, scratch);
    }

    pub fn take_wait(&mut self) -> Option<Wait<M>> {
        self.wait.take()
    }

    fn push_action(&mut self, action: Action<M>) -> Result<(), EffectsError> {
        if self.sealed {
            return Err(EffectsError::TurnSealed);
        }

        if self.actions.len() == self.max_actions {
            return Err(EffectsError::ActionsFull);
        }

        self.actions.push(action);
        Ok(())
    }

    fn set_wait(&mut self, wait: Wait<M>) -> Result<(), EffectsError> {
        if self.sealed {
            return Err(EffectsError::TurnSealed);
        }

        if self.wait.is_some() {
            return Err(EffectsError::WaitAlreadySet);
        }

        self.wait = Some(wait);
        self.sealed = true;
        Ok(())
    }
}