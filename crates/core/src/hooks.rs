use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::Result;
use crate::types::{Memory, State};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookPhase {
    IncomingBeforeCompose,
    PreShouldRespond,
    PostShouldRespond,
    PreGenerate,
    PostGenerate,
    PreEvaluate,
    PostEvaluate,
    PreAction,
    PostAction,
}

impl fmt::Display for HookPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncomingBeforeCompose => write!(f, "incoming_before_compose"),
            Self::PreShouldRespond => write!(f, "pre_should_respond"),
            Self::PostShouldRespond => write!(f, "post_should_respond"),
            Self::PreGenerate => write!(f, "pre_generate"),
            Self::PostGenerate => write!(f, "post_generate"),
            Self::PreEvaluate => write!(f, "pre_evaluate"),
            Self::PostEvaluate => write!(f, "post_evaluate"),
            Self::PreAction => write!(f, "pre_action"),
            Self::PostAction => write!(f, "post_action"),
        }
    }
}

pub type HookHandler = Arc<
    dyn Fn(&Memory, &State) -> Pin<Box<dyn Future<Output = Result<HookAction>> + Send>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookAction {
    Continue,
    Skip,
    Abort,
}

pub struct PipelineHook {
    pub name: String,
    pub phase: HookPhase,
    pub priority: i32,
    pub handler: HookHandler,
}

impl PipelineHook {
    pub fn new(
        name: impl Into<String>,
        phase: HookPhase,
        handler: HookHandler,
    ) -> Self {
        Self {
            name: name.into(),
            phase,
            priority: 0,
            handler,
        }
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

#[derive(Default)]
pub struct HookRegistry {
    hooks: Vec<PipelineHook>,
}

impl HookRegistry {
    pub fn new() -> Self {
        Self { hooks: vec![] }
    }

    pub fn register(&mut self, hook: PipelineHook) {
        self.hooks.push(hook);
        self.hooks.sort_by_key(|h| h.priority);
    }

    pub async fn run(&self, phase: HookPhase, message: &Memory, state: &State) -> Result<HookAction> {
        for hook in self.hooks.iter().filter(|h| h.phase == phase) {
            let action = (hook.handler)(message, state).await?;
            match action {
                HookAction::Continue => continue,
                HookAction::Skip => return Ok(HookAction::Skip),
                HookAction::Abort => return Ok(HookAction::Abort),
            }
        }
        Ok(HookAction::Continue)
    }

    pub fn hooks_for_phase(&self, phase: HookPhase) -> Vec<&PipelineHook> {
        self.hooks.iter().filter(|h| h.phase == phase).collect()
    }
}
