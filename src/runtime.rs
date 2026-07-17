use std::collections::VecDeque;

use crate::arena::{Arena, ArenaError, Handle};
use crate::completion::{CompletionArena, CompletionError, CompletionHandle, CompletionState};
use crate::effects::{Effect, EffectsError, RuntimeMessage, TurnEffects};
use crate::io::{DriverCompletion, IoDriver, IoError, IoResult, Operation};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Envelope {
    target: Handle,
    message: RuntimeMessage,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Arena(ArenaError),
    Completion(CompletionError),
    Effects(EffectsError),
    Io(IoError),
    QueueFull,
}

impl From<ArenaError> for RuntimeError {
    fn from(value: ArenaError) -> Self {
        Self::Arena(value)
    }
}

impl From<CompletionError> for RuntimeError {
    fn from(value: CompletionError) -> Self {
        Self::Completion(value)
    }
}

impl From<EffectsError> for RuntimeError {
    fn from(value: EffectsError) -> Self {
        Self::Effects(value)
    }
}

impl From<IoError> for RuntimeError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

pub struct IoContext<'a> {
    completions: &'a mut CompletionArena<IoResult>,
    owner: Handle,
}

impl IoContext<'_> {
    pub fn acquire(&mut self) -> Option<CompletionHandle> {
        self.completions.acquire(self.owner).ok()
    }

    #[allow(dead_code)]
    pub fn release(&mut self, completion: CompletionHandle) -> Result<(), CompletionError> {
        self.completions.release(self.owner, completion)
    }

    pub fn take_result(&mut self, completion: CompletionHandle) -> Option<IoResult> {
        self.completions.take_result(self.owner, completion).ok()
    }
}

pub trait Isolate {
    fn handle(
        &mut self,
        msg: RuntimeMessage,
        io: &mut IoContext<'_>,
        effects: &mut TurnEffects,
    ) -> Result<(), RuntimeError>;

    fn destroy(
        &mut self,
        io: &mut IoContext<'_>,
        effects: &mut TurnEffects,
    ) -> Result<(), RuntimeError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    Idle,
    ProcessedOne,
    AdvancedIo,
    DroppedInvalid,
}

pub struct IoLoop<D> {
    driver: D,
    completions: CompletionArena<IoResult>,
    completed: Vec<DriverCompletion>,
    ready: VecDeque<CompletionHandle>,
    ready_capacity: usize,
}

impl<D> IoLoop<D>
where
    D: IoDriver,
{
    pub fn new(driver: D, completion_capacity: usize) -> Self {
        Self {
            driver,
            completions: CompletionArena::with_capacity(completion_capacity),
            completed: Vec::with_capacity(completion_capacity),
            ready: VecDeque::with_capacity(completion_capacity),
            ready_capacity: completion_capacity,
        }
    }

    pub fn step(&mut self) -> Result<bool, RuntimeError> {
        self.completed.clear();
        let available = self.ready_capacity.saturating_sub(self.ready.len());
        let progressed = self.driver.step(available, &mut self.completed)?;

        for DriverCompletion { completion, result } in self.completed.drain(..) {
            let owner = self.completions.owner(completion)?;
            let state = self.completions.state(completion)?;
            self.completions.complete(completion, result)?;

            if state == CompletionState::Cancelling {
                self.completions.release(owner, completion)?;
            } else {
                debug_assert_eq!(state, CompletionState::Submitted);
                self.ready.push_back(completion);
            }
        }

        Ok(progressed)
    }

    pub fn has_pending(&self) -> bool {
        self.driver.has_pending() || !self.ready.is_empty()
    }

    fn submit(
        &mut self,
        owner: Handle,
        completion: CompletionHandle,
        op: Operation,
    ) -> Result<(), RuntimeError> {
        self.completions.submit(owner, completion)?;
        self.driver.submit(completion, op)?;
        Ok(())
    }

    fn cancel(&mut self, owner: Handle, completion: CompletionHandle) -> Result<(), RuntimeError> {
        match self.completions.begin_cancel(owner, completion)? {
            CompletionState::Idle | CompletionState::Ready => {
                self.completions.release(owner, completion)?;
            }
            CompletionState::Cancelling => {
                self.driver.cancel(completion)?;
            }
            CompletionState::Submitted => unreachable!("begin_cancel advances submitted slots"),
        }
        Ok(())
    }

    fn pop_ready(&mut self) -> Option<CompletionHandle> {
        self.ready.pop_front()
    }

    fn owner(&self, completion: CompletionHandle) -> Result<Handle, CompletionError> {
        self.completions.owner(completion)
    }

    fn has_owner(&self, owner: Handle) -> bool {
        self.completions.has_owner(owner)
    }
}

struct IsolateSlot<I> {
    isolate: I,
    destroying: bool,
}

pub struct Server<I>
where
    I: Isolate,
{
    arena: Arena<IsolateSlot<I>>,
    queue: VecDeque<Envelope>,
    queue_capacity: usize,
    effects: TurnEffects,
    effect_scratch: Vec<Effect>,
    destroying: Vec<Handle>,
}

impl<I> Server<I>
where
    I: Isolate,
{
    pub fn new(isolate_capacity: usize, queue_capacity: usize, effect_capacity: usize) -> Self {
        Self {
            arena: Arena::with_capacity(isolate_capacity),
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
            effects: TurnEffects::with_capacity(effect_capacity),
            effect_scratch: Vec::with_capacity(effect_capacity),
            destroying: Vec::with_capacity(isolate_capacity),
        }
    }

    pub fn spawn(&mut self, isolate: I) -> Result<Handle, RuntimeError> {
        let handle = self.arena.insert(IsolateSlot {
            isolate,
            destroying: false,
        })?;
        if let Err(error) = self.enqueue(handle, RuntimeMessage::Init) {
            let _ = self.arena.remove(handle);
            return Err(error);
        }
        Ok(handle)
    }

    #[allow(dead_code)]
    pub fn shutdown(&mut self, target: Handle) -> Result<(), RuntimeError> {
        self.enqueue(target, RuntimeMessage::Shutdown)
    }

    pub fn step<D>(&mut self, io: &mut IoLoop<D>) -> Result<StepResult, RuntimeError>
    where
        D: IoDriver,
    {
        self.finalize_destroyed(io)?;
        let routed = self.route_ready(io)?;

        let Some(envelope) = self.queue.pop_front() else {
            return if routed || io.has_pending() {
                Ok(StepResult::AdvancedIo)
            } else {
                Ok(StepResult::Idle)
            };
        };

        let Some(slot) = self.arena.get_mut(envelope.target) else {
            return Ok(StepResult::DroppedInvalid);
        };
        if slot.destroying {
            return Ok(StepResult::DroppedInvalid);
        }

        self.effects.reset();
        {
            let mut context = IoContext {
                completions: &mut io.completions,
                owner: envelope.target,
            };
            slot.isolate
                .handle(envelope.message, &mut context, &mut self.effects)?;
        }

        self.effects.swap_effects(&mut self.effect_scratch);
        let destroy_requested = self.interpret_effects(io, envelope.target)?;
        if destroy_requested {
            self.start_destroy(io, envelope.target)?;
        }
        self.finalize_destroyed(io)?;

        Ok(StepResult::ProcessedOne)
    }

    pub fn run_until_idle<D>(&mut self, io: &mut IoLoop<D>) -> Result<usize, RuntimeError>
    where
        D: IoDriver,
    {
        let mut processed = 0;
        loop {
            match self.step(io)? {
                StepResult::Idle => return Ok(processed),
                StepResult::ProcessedOne => processed += 1,
                StepResult::AdvancedIo | StepResult::DroppedInvalid => {}
            }
            io.step()?;
        }
    }

    #[allow(dead_code)]
    pub fn isolate_count(&self) -> usize {
        self.arena.len()
    }

    fn enqueue(&mut self, target: Handle, message: RuntimeMessage) -> Result<(), RuntimeError> {
        if self.queue.len() == self.queue_capacity {
            return Err(RuntimeError::QueueFull);
        }
        self.queue.push_back(Envelope { target, message });
        Ok(())
    }

    fn route_ready<D>(&mut self, io: &mut IoLoop<D>) -> Result<bool, RuntimeError>
    where
        D: IoDriver,
    {
        let mut routed = false;
        while self.queue.len() < self.queue_capacity {
            let Some(completion) = io.pop_ready() else {
                break;
            };
            let Ok(owner) = io.owner(completion) else {
                continue;
            };
            self.queue.push_back(Envelope {
                target: owner,
                message: RuntimeMessage::IoCompleted(completion),
            });
            routed = true;
        }
        Ok(routed)
    }

    fn interpret_effects<D>(
        &mut self,
        io: &mut IoLoop<D>,
        owner: Handle,
    ) -> Result<bool, RuntimeError>
    where
        D: IoDriver,
    {
        let submit_count = self
            .effect_scratch
            .iter()
            .filter(|effect| matches!(effect, Effect::Submit { .. }))
            .count();
        if !io.driver.can_submit(submit_count) {
            return Err(RuntimeError::Io(IoError::DriverFull));
        }

        let mut destroy_requested = false;
        for effect in self.effect_scratch.drain(..) {
            match effect {
                Effect::Submit { completion, op } => io.submit(owner, completion, op)?,
                Effect::Cancel { completion } => io.cancel(owner, completion)?,
                Effect::DestroySelf => destroy_requested = true,
            }
        }
        Ok(destroy_requested)
    }

    fn start_destroy<D>(&mut self, io: &mut IoLoop<D>, owner: Handle) -> Result<(), RuntimeError>
    where
        D: IoDriver,
    {
        let Some(slot) = self.arena.get_mut(owner) else {
            return Ok(());
        };
        if slot.destroying {
            return Ok(());
        }
        slot.destroying = true;

        self.effects.reset();
        {
            let mut context = IoContext {
                completions: &mut io.completions,
                owner,
            };
            slot.isolate.destroy(&mut context, &mut self.effects)?;
        }
        self.effects.swap_effects(&mut self.effect_scratch);
        let destroy_requested = self.interpret_effects(io, owner)?;
        debug_assert!(
            !destroy_requested,
            "destroy must not request destruction again"
        );
        self.destroying.push(owner);
        Ok(())
    }

    fn finalize_destroyed<D>(&mut self, io: &IoLoop<D>) -> Result<(), RuntimeError>
    where
        D: IoDriver,
    {
        let mut index = 0;
        while index < self.destroying.len() {
            let owner = self.destroying[index];
            if io.has_owner(owner) {
                index += 1;
                continue;
            }
            if self.arena.contains(owner) {
                let _ = self.arena.remove(owner)?;
            }
            self.destroying.swap_remove(index);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{IoContext, IoLoop, Isolate, RuntimeError, Server};
    use crate::completion::CompletionHandle;
    use crate::effects::{RuntimeMessage, TurnEffects};
    use crate::io::{FakeIoDriver, IoResult, Operation, TimerOp};

    struct TimerOnce {
        timer: CompletionHandle,
        completed: bool,
    }

    impl Isolate for TimerOnce {
        fn handle(
            &mut self,
            msg: RuntimeMessage,
            io: &mut IoContext<'_>,
            effects: &mut TurnEffects,
        ) -> Result<(), RuntimeError> {
            match msg {
                RuntimeMessage::Init => {
                    self.timer = io.acquire().expect("completion capacity");
                    effects.submit(self.timer, Operation::Timer(TimerOp { ticks: 1 }))?;
                }
                RuntimeMessage::IoCompleted(completion) if completion == self.timer => {
                    assert_eq!(io.take_result(completion), Some(IoResult::Timer(Ok(()))));
                    self.completed = true;
                    effects.destroy_self()?;
                }
                RuntimeMessage::IoCompleted(_) => {}
                RuntimeMessage::Shutdown => effects.destroy_self()?,
            }
            Ok(())
        }

        fn destroy(
            &mut self,
            _io: &mut IoContext<'_>,
            effects: &mut TurnEffects,
        ) -> Result<(), RuntimeError> {
            effects.cancel(self.timer)?;
            Ok(())
        }
    }

    struct DestroyWhilePending {
        completion: CompletionHandle,
        destroyed: Rc<Cell<usize>>,
    }

    impl Isolate for DestroyWhilePending {
        fn handle(
            &mut self,
            msg: RuntimeMessage,
            io: &mut IoContext<'_>,
            effects: &mut TurnEffects,
        ) -> Result<(), RuntimeError> {
            if msg == RuntimeMessage::Init {
                self.completion = io.acquire().expect("completion capacity");
                effects.submit(self.completion, Operation::Timer(TimerOp { ticks: 1 }))?;
                effects.destroy_self()?;
            }
            Ok(())
        }

        fn destroy(
            &mut self,
            _io: &mut IoContext<'_>,
            effects: &mut TurnEffects,
        ) -> Result<(), RuntimeError> {
            self.destroyed.set(self.destroyed.get() + 1);
            effects.cancel(self.completion)?;
            Ok(())
        }
    }

    #[test]
    fn server_routes_completion_to_its_owner_and_reuses_lifecycle() {
        let mut server = Server::new(1, 2, 2);
        let mut io = IoLoop::new(FakeIoDriver::new(2), 2);
        server
            .spawn(TimerOnce {
                timer: CompletionHandle::INVALID,
                completed: false,
            })
            .unwrap();

        assert_eq!(server.run_until_idle(&mut io).unwrap(), 2);
        assert_eq!(server.isolate_count(), 0);
    }

    #[test]
    fn destroying_isolate_waits_for_cancel_completion_before_release() {
        let destroyed = Rc::new(Cell::new(0));
        let mut server = Server::new(1, 2, 3);
        let mut io = IoLoop::new(FakeIoDriver::new(2), 2);
        server
            .spawn(DestroyWhilePending {
                completion: CompletionHandle::INVALID,
                destroyed: Rc::clone(&destroyed),
            })
            .unwrap();

        assert_eq!(server.run_until_idle(&mut io).unwrap(), 1);
        assert_eq!(destroyed.get(), 1);
        assert_eq!(server.isolate_count(), 0);
    }

    #[test]
    fn shutdown_uses_destroy_lifecycle_and_drops_queued_completion() {
        let mut server = Server::new(1, 3, 2);
        let mut io = IoLoop::new(FakeIoDriver::new(2), 2);
        let handle = server
            .spawn(TimerOnce {
                timer: CompletionHandle::INVALID,
                completed: false,
            })
            .unwrap();
        server.shutdown(handle).unwrap();

        assert_eq!(server.run_until_idle(&mut io).unwrap(), 2);
        assert_eq!(server.isolate_count(), 0);
    }
}
