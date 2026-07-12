use crate::arena::Handle;

use super::action::Action;
use super::error::EffectsError;
use super::message::RuntimeMessage;
use super::token::OpToken;
use super::wait::Wait;

/// How a group of waits armed together should complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupPolicy {
    /// The first member to complete wins; the runtime cancels the rest and
    /// delivers only the winner's completion.
    Select,
    /// All members must complete; the runtime suppresses individual completions
    /// and delivers a single join completion once the last member finishes.
    Join,
}

/// One armed wait produced during a turn, handed to the scheduler for
/// registration and submission to the backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Armed<M> {
    pub token: OpToken,
    pub wait: Wait,
    /// The message delivered when this wait completes, or `None` for a group
    /// member whose completion is suppressed by its policy (e.g. join members).
    pub completion: Option<RuntimeMessage<M>>,
    pub group: Option<u32>,
}

/// A group of armed waits sharing a completion policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupSpec<M> {
    pub id: u32,
    pub policy: GroupPolicy,
    pub members: Vec<OpToken>,
    /// The token and message delivered once a `Join` group finishes.
    pub join: Option<(OpToken, RuntimeMessage<M>)>,
}

/// Per-turn builder an isolate uses to describe what should happen next.
///
/// A turn may emit any number of immediate [`Action`]s and arm any number of
/// concurrent waits (a "wait set"). Each armed wait returns an [`OpToken`] the
/// isolate can use to correlate completions or cancel the operation.
pub struct TurnEffects<M> {
    actions: Vec<Action<M>>,
    armed: Vec<Armed<M>>,
    groups: Vec<GroupSpec<M>>,
    max_actions: usize,
    max_waits: usize,
    next_token: u32,
    next_group: u32,
}

impl<M> TurnEffects<M> {
    pub fn with_capacity(max_actions: usize, max_waits: usize) -> Self {
        Self {
            actions: Vec::with_capacity(max_actions),
            armed: Vec::with_capacity(max_waits),
            groups: Vec::new(),
            max_actions,
            max_waits,
            next_token: 0,
            next_group: 0,
        }
    }

    /// Clear per-turn state and seed the token/group counters for the upcoming
    /// turn so identifiers stay globally monotonic across the whole runtime.
    pub fn reset(&mut self, next_token: u32, next_group: u32) {
        self.actions.clear();
        self.armed.clear();
        self.groups.clear();
        self.next_token = next_token;
        self.next_group = next_group;
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

    /// Cancel a previously armed operation. Cancelling a completed or unknown
    /// token is a no-op once the scheduler applies it.
    pub fn cancel(&mut self, token: OpToken) -> Result<(), EffectsError> {
        self.push_action(Action::Cancel { token })
    }

    pub fn wait_accept(&mut self, listener: u32, completion: M) -> Result<OpToken, EffectsError> {
        self.arm(Wait::Accept { listener }, Some(completion), None)
    }

    pub fn wait_recv(&mut self, source: u32, completion: M) -> Result<OpToken, EffectsError> {
        self.arm(Wait::Recv { source }, Some(completion), None)
    }

    pub fn wait_write(&mut self, sink: u32, completion: M) -> Result<OpToken, EffectsError> {
        self.arm(Wait::Write { sink }, Some(completion), None)
    }

    pub fn wait_timer(&mut self, ticks: u32, completion: M) -> Result<OpToken, EffectsError> {
        self.arm(Wait::Timer { ticks }, Some(completion), None)
    }

    /// Arm several waits as a race. The first to complete wins; the runtime
    /// cancels the losers and delivers only the winner's completion. Returns
    /// the member tokens in the order supplied.
    pub fn select(
        &mut self,
        members: impl IntoIterator<Item = (Wait, M)>,
    ) -> Result<Vec<OpToken>, EffectsError> {
        let group = self.next_group;
        self.next_group += 1;

        let mut tokens = Vec::new();
        for (wait, completion) in members {
            let token = self.arm(wait, Some(completion), Some(group))?;
            tokens.push(token);
        }

        self.groups.push(GroupSpec {
            id: group,
            policy: GroupPolicy::Select,
            members: tokens.clone(),
            join: None,
        });
        Ok(tokens)
    }

    /// Arm several waits as a join. Individual completions are suppressed; the
    /// runtime delivers a single `completion` message (tagged with the returned
    /// token) once every member has completed.
    pub fn join(
        &mut self,
        members: impl IntoIterator<Item = Wait>,
        completion: M,
    ) -> Result<OpToken, EffectsError> {
        let group = self.next_group;
        self.next_group += 1;

        let mut tokens = Vec::new();
        for wait in members {
            let token = self.arm(wait, None, Some(group))?;
            tokens.push(token);
        }

        let join_token = self.mint();
        self.groups.push(GroupSpec {
            id: group,
            policy: GroupPolicy::Join,
            members: tokens,
            join: Some((join_token, RuntimeMessage::User(completion))),
        });
        Ok(join_token)
    }

    pub fn swap_actions(&mut self, scratch: &mut Vec<Action<M>>) {
        scratch.clear();
        std::mem::swap(&mut self.actions, scratch);
    }

    pub fn swap_armed(&mut self, scratch: &mut Vec<Armed<M>>) {
        scratch.clear();
        std::mem::swap(&mut self.armed, scratch);
    }

    pub fn swap_groups(&mut self, scratch: &mut Vec<GroupSpec<M>>) {
        scratch.clear();
        std::mem::swap(&mut self.groups, scratch);
    }

    /// The next token value after this turn, so the scheduler can persist the
    /// counter and keep tokens monotonic across turns.
    pub fn next_token(&self) -> u32 {
        self.next_token
    }

    /// The next group id after this turn, persisted by the scheduler.
    pub fn next_group(&self) -> u32 {
        self.next_group
    }

    fn mint(&mut self) -> OpToken {
        let token = OpToken(self.next_token);
        self.next_token += 1;
        token
    }

    fn arm(
        &mut self,
        wait: Wait,
        completion: Option<M>,
        group: Option<u32>,
    ) -> Result<OpToken, EffectsError> {
        if self.armed.len() == self.max_waits {
            return Err(EffectsError::WaitsFull);
        }

        let token = self.mint();
        self.armed.push(Armed {
            token,
            wait,
            completion: completion.map(RuntimeMessage::User),
            group,
        });
        Ok(token)
    }

    fn push_action(&mut self, action: Action<M>) -> Result<(), EffectsError> {
        if self.actions.len() == self.max_actions {
            return Err(EffectsError::ActionsFull);
        }

        self.actions.push(action);
        Ok(())
    }
}
