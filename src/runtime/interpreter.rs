use crate::arena::Arena;
use crate::effects::{Action, Armed, GroupSpec, RuntimeMessage};
use crate::io::WaitBackend;

use super::envelope::Envelope;
use super::error::RuntimeError;
use super::inflight::WaitRegistry;
use super::isolate::Isolate;

/// Applies a turn's effects atomically.
///
/// The interpreter first validates that every effect can be applied (queue has
/// room for all sends, the backend can accept all newly armed waits). Only if
/// validation passes does it commit: draining actions, opening groups, and
/// submitting waits. This keeps a turn all-or-nothing, so a mid-turn capacity
/// failure never leaves the runtime in a partially-applied state.
pub(crate) struct EffectInterpreter;

impl EffectInterpreter {
    pub(crate) fn interpret_turn<I, B>(
        current: crate::arena::Handle,
        arena: &mut Arena<I>,
        queue: &mut std::collections::VecDeque<Envelope<I::Message>>,
        queue_capacity: usize,
        registry: &mut WaitRegistry<I::Message>,
        backend: &mut B,
        actions: &mut Vec<Action<I::Message>>,
        armed: &mut Vec<Armed<I::Message>>,
        groups: &mut Vec<GroupSpec<I::Message>>,
    ) -> Result<(), RuntimeError>
    where
        I: Isolate,
        I::Message: Clone,
        B: WaitBackend,
    {
        // --- validate ---------------------------------------------------------
        let mut send_slots = 0usize;
        for action in actions.iter() {
            match action {
                Action::Send { .. } | Action::SendSelf { .. } => send_slots += 1,
                Action::DestroySelf | Action::Cancel { .. } => {}
            }
        }

        if queue.len() + send_slots > queue_capacity {
            return Err(RuntimeError::QueueFull);
        }

        if !backend.can_submit(armed.len()) {
            return Err(RuntimeError::WaitQueueFull);
        }

        // --- commit -----------------------------------------------------------
        // Arm waits first so a same-turn `Cancel` or `DestroySelf` can see and
        // drop them; otherwise a cancel would target a not-yet-registered token.
        for group in groups.drain(..) {
            registry.open_group(group.id, group.policy, current, group.members, group.join);
        }

        for op in armed.drain(..) {
            registry.register(op.token, current, op.completion, op.group);
            backend.submit(op.token, op.wait)?;
        }

        for action in actions.drain(..) {
            match action {
                Action::Send { target, message } => {
                    queue.push_back(Envelope {
                        target,
                        message,
                        token: None,
                    });
                }
                Action::SendSelf { message } => {
                    queue.push_back(Envelope {
                        target: current,
                        message: RuntimeMessage::User(message),
                        token: None,
                    });
                }
                Action::DestroySelf => {
                    registry.purge_owner(current, backend);
                    arena.remove(current)?;
                }
                Action::Cancel { token } => {
                    registry.cancel_token(token, backend);
                }
            }
        }

        Ok(())
    }
}
