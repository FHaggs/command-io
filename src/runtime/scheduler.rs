use std::collections::VecDeque;

use crate::arena::{Arena, Handle};
use crate::effects::{Action, Armed, GroupSpec, RuntimeMessage, TurnEffects};
use crate::io::{FakeWaitBackend, WaitBackend};

use super::envelope::Envelope;
use super::error::RuntimeError;
use super::inflight::WaitRegistry;
use super::interpreter::EffectInterpreter;
use super::isolate::Isolate;
use super::step::StepResult;

/// Cooperative scheduler that drives isolates to completion.
///
/// The scheduler owns the message queue, the per-turn effect builder, the wait
/// backend, and the [`WaitRegistry`] that maps completing tokens back to the
/// isolates and messages that armed them.
pub struct Scheduler<I: Isolate, B: WaitBackend = FakeWaitBackend> {
    arena: Arena<I>,
    queue: VecDeque<Envelope<I::Message>>,
    queue_capacity: usize,
    effects: TurnEffects<I::Message>,
    action_scratch: Vec<Action<I::Message>>,
    armed_scratch: Vec<Armed<I::Message>>,
    groups_scratch: Vec<GroupSpec<I::Message>>,
    registry: WaitRegistry<I::Message>,
    ready_scratch: Vec<crate::effects::OpToken>,
    waits: B,
    next_token: u32,
    next_group: u32,
}

impl<I> Scheduler<I, FakeWaitBackend>
where
    I: Isolate,
    I::Message: Clone,
{
    pub fn new(arena_capacity: usize, queue_capacity: usize, action_capacity: usize) -> Self {
        Self::new_with_backend(
            arena_capacity,
            queue_capacity,
            action_capacity,
            FakeWaitBackend::new(queue_capacity),
        )
    }
}

impl<I, B> Scheduler<I, B>
where
    I: Isolate,
    I::Message: Clone,
    B: WaitBackend,
{
    pub fn new_with_backend(
        arena_capacity: usize,
        queue_capacity: usize,
        action_capacity: usize,
        backend: B,
    ) -> Self {
        Self {
            arena: Arena::with_capacity(arena_capacity),
            queue: VecDeque::with_capacity(queue_capacity),
            queue_capacity,
            effects: TurnEffects::with_capacity(action_capacity, action_capacity),
            action_scratch: Vec::with_capacity(action_capacity),
            armed_scratch: Vec::with_capacity(action_capacity),
            groups_scratch: Vec::new(),
            registry: WaitRegistry::new(),
            ready_scratch: Vec::with_capacity(action_capacity),
            waits: backend,
            next_token: 0,
            next_group: 0,
        }
    }

    /// Insert an isolate and enqueue its `Init` message.
    pub fn spawn(&mut self, isolate: I) -> Result<Handle, RuntimeError> {
        let handle = self.arena.insert(isolate)?;
        enqueue_envelope(
            &mut self.queue,
            self.queue_capacity,
            handle,
            RuntimeMessage::Init,
        )?;
        Ok(handle)
    }

    /// Enqueue a user message addressed to `target`.
    pub fn enqueue(&mut self, target: Handle, message: I::Message) -> Result<(), RuntimeError> {
        enqueue_envelope(
            &mut self.queue,
            self.queue_capacity,
            target,
            RuntimeMessage::User(message),
        )
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn isolate_mut(&mut self, handle: Handle) -> Option<&mut I> {
        self.arena.get_mut(handle)
    }

    /// Advance the runtime by a single step.
    pub fn run_once(&mut self) -> Result<StepResult, RuntimeError> {
        // Deliver any completions that were ready but could not fit last step.
        self.registry
            .pump(&mut self.queue, self.queue_capacity, &mut self.waits);

        // Poll the backend for freshly ready operations and route them.
        self.ready_scratch.clear();
        let progressed = self.waits.poll(&mut self.ready_scratch)?;
        self.registry.enqueue_ready(self.ready_scratch.drain(..));
        self.registry
            .pump(&mut self.queue, self.queue_capacity, &mut self.waits);

        let Some(envelope) = self.queue.pop_front() else {
            if progressed {
                return Ok(StepResult::AdvancedWaits);
            }
            if self.waits.has_pending() || !self.registry.is_empty() {
                return Ok(StepResult::Blocked);
            }
            return Ok(StepResult::Idle);
        };

        if !self.arena.contains(envelope.target) {
            return Ok(StepResult::DroppedInvalid);
        }

        self.effects.reset(self.next_token, self.next_group);
        {
            let isolate = self
                .arena
                .get_mut(envelope.target)
                .expect("target presence checked above");
            isolate.handle(envelope.message, envelope.token, &mut self.effects)?;
        }
        self.next_token = self.effects.next_token();
        self.next_group = self.effects.next_group();

        self.effects.swap_actions(&mut self.action_scratch);
        self.effects.swap_armed(&mut self.armed_scratch);
        self.effects.swap_groups(&mut self.groups_scratch);

        EffectInterpreter::interpret_turn(
            envelope.target,
            &mut self.arena,
            &mut self.queue,
            self.queue_capacity,
            &mut self.registry,
            &mut self.waits,
            &mut self.action_scratch,
            &mut self.armed_scratch,
            &mut self.groups_scratch,
        )?;

        Ok(StepResult::ProcessedOne)
    }

    /// Run until the queue drains and no further progress can be made.
    pub fn run_until_idle(&mut self) -> Result<usize, RuntimeError> {
        let mut processed = 0;

        loop {
            match self.run_once()? {
                StepResult::ProcessedOne => processed += 1,
                StepResult::AdvancedWaits | StepResult::DroppedInvalid => {}
                StepResult::Idle => break,
                StepResult::Blocked => {
                    if !self.waits.block_until_event()? {
                        break;
                    }
                }
            }
        }

        Ok(processed)
    }
}

fn enqueue_envelope<M>(
    queue: &mut VecDeque<Envelope<M>>,
    capacity: usize,
    target: Handle,
    message: RuntimeMessage<M>,
) -> Result<(), RuntimeError> {
    if queue.len() == capacity {
        return Err(RuntimeError::QueueFull);
    }

    queue.push_back(Envelope {
        target,
        message,
        token: None,
    });
    Ok(())
}
