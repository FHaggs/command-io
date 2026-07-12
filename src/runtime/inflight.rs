use std::collections::{HashMap, VecDeque};

use crate::arena::Handle;
use crate::effects::{GroupPolicy, OpToken, RuntimeMessage};
use crate::io::WaitBackend;

use super::envelope::Envelope;

/// One armed, not-yet-completed operation.
struct InFlight<M> {
    target: Handle,
    /// The message to deliver on completion, or `None` for a group member
    /// whose individual completion is suppressed (join members).
    completion: Option<RuntimeMessage<M>>,
    group: Option<u32>,
}

/// A group of operations sharing a completion policy.
struct Group<M> {
    policy: GroupPolicy,
    target: Handle,
    remaining: usize,
    members: Vec<OpToken>,
    /// The token and message delivered once a `Join` group finishes.
    join: Option<(OpToken, RuntimeMessage<M>)>,
}

/// Scheduler-side registry that owns completion messages and group policy for
/// every in-flight operation.
///
/// The backend only knows tokens; this registry maps a completing token back to
/// the isolate that armed it, the message to deliver, and any group semantics
/// (select cancels losers, join waits for all members).
///
/// NOTE: this uses `HashMap` for clarity in Phase 1. It is a deliberate
/// deviation from the "no allocation in the hot path" goal and is expected to
/// be replaced with a slab/arena keyed by token index in a later phase.
pub(crate) struct WaitRegistry<M> {
    in_flight: HashMap<OpToken, InFlight<M>>,
    groups: HashMap<u32, Group<M>>,
    ready: VecDeque<OpToken>,
}

impl<M> WaitRegistry<M> {
    pub fn new() -> Self {
        Self {
            in_flight: HashMap::new(),
            groups: HashMap::new(),
            ready: VecDeque::new(),
        }
    }

    /// Register a single armed operation.
    pub fn register(
        &mut self,
        token: OpToken,
        target: Handle,
        completion: Option<RuntimeMessage<M>>,
        group: Option<u32>,
    ) {
        self.in_flight.insert(
            token,
            InFlight {
                target,
                completion,
                group,
            },
        );
    }

    /// Open a group before its members are registered.
    pub fn open_group(
        &mut self,
        id: u32,
        policy: GroupPolicy,
        target: Handle,
        members: Vec<OpToken>,
        join: Option<(OpToken, RuntimeMessage<M>)>,
    ) {
        let remaining = members.len();
        self.groups.insert(
            id,
            Group {
                policy,
                target,
                remaining,
                members,
                join,
            },
        );
    }

    /// Cancel a single token, telling the backend to drop it. If the token
    /// belongs to a group, its membership is updated and empty groups removed.
    pub fn cancel_token<B: WaitBackend>(&mut self, token: OpToken, backend: &mut B) {
        let Some(rec) = self.in_flight.remove(&token) else {
            return;
        };
        backend.cancel(token);

        if let Some(gid) = rec.group {
            self.remove_group_member(gid, token);
        }
    }

    /// Cancel every operation owned by `target` (used when an isolate destroys
    /// itself), dropping them from the backend too.
    pub fn purge_owner<B: WaitBackend>(&mut self, target: Handle, backend: &mut B) {
        let doomed: Vec<OpToken> = self
            .in_flight
            .iter()
            .filter(|(_, rec)| rec.target == target)
            .map(|(token, _)| *token)
            .collect();

        for token in doomed {
            self.in_flight.remove(&token);
            backend.cancel(token);
        }

        self.groups.retain(|_, group| group.target != target);

        let in_flight = &self.in_flight;
        self.ready.retain(|token| in_flight.contains_key(token));
    }

    /// Queue backend-reported ready tokens for delivery.
    pub fn enqueue_ready(&mut self, tokens: impl IntoIterator<Item = OpToken>) {
        self.ready.extend(tokens);
    }

    /// Deliver ready completions into `queue` while it has spare capacity,
    /// cancelling losing siblings on the `backend` as select groups resolve.
    pub fn pump<B: WaitBackend>(
        &mut self,
        queue: &mut VecDeque<Envelope<M>>,
        capacity: usize,
        backend: &mut B,
    ) {
        while queue.len() < capacity {
            let Some(token) = self.ready.pop_front() else {
                break;
            };
            self.deliver(token, queue, backend);
        }
    }

    /// Whether any operation is still registered (in-flight or awaiting delivery).
    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty() && self.ready.is_empty()
    }

    fn deliver<B: WaitBackend>(
        &mut self,
        token: OpToken,
        queue: &mut VecDeque<Envelope<M>>,
        backend: &mut B,
    ) {
        // Already cancelled or delivered: skip silently.
        let Some(rec) = self.in_flight.remove(&token) else {
            return;
        };

        match rec.group {
            None => {
                let message = rec
                    .completion
                    .expect("ungrouped waits always carry a completion");
                queue.push_back(Envelope {
                    target: rec.target,
                    message,
                    token: Some(token),
                });
            }
            Some(gid) => self.deliver_grouped(gid, token, rec, queue, backend),
        }
    }

    fn deliver_grouped<B: WaitBackend>(
        &mut self,
        gid: u32,
        token: OpToken,
        rec: InFlight<M>,
        queue: &mut VecDeque<Envelope<M>>,
        backend: &mut B,
    ) {
        let policy = match self.groups.get(&gid) {
            Some(group) => group.policy,
            None => return,
        };

        match policy {
            GroupPolicy::Select => {
                // Winner delivers its own completion; losers are cancelled.
                let group = self.groups.remove(&gid).expect("group present");
                let message = rec
                    .completion
                    .expect("select members always carry a completion");
                queue.push_back(Envelope {
                    target: rec.target,
                    message,
                    token: Some(token),
                });

                for member in group.members {
                    if member == token {
                        continue;
                    }
                    if self.in_flight.remove(&member).is_some() {
                        backend.cancel(member);
                    }
                }
            }
            GroupPolicy::Join => {
                let finished = {
                    let group = self.groups.get_mut(&gid).expect("group present");
                    group.remaining -= 1;
                    group.members.retain(|m| *m != token);
                    group.remaining == 0
                };

                if finished {
                    let group = self.groups.remove(&gid).expect("group present");
                    let (join_token, message) =
                        group.join.expect("join groups always carry a completion");
                    queue.push_back(Envelope {
                        target: group.target,
                        message,
                        token: Some(join_token),
                    });
                }
            }
        }
    }

    fn remove_group_member(&mut self, gid: u32, token: OpToken) {
        let Some(group) = self.groups.get_mut(&gid) else {
            return;
        };

        group.members.retain(|m| *m != token);
        group.remaining = group.remaining.saturating_sub(1);

        if group.members.is_empty() {
            self.groups.remove(&gid);
        }
    }
}
