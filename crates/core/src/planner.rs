use serde::{Deserialize, Serialize};

use crate::types::ActionResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepStatus {
    Pending,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionPlanStep {
    pub action: String,
    pub reasoning: Option<String>,
    pub status: PlanStepStatus,
    #[serde(skip)]
    pub result: Option<ActionResult>,
}

impl ActionPlanStep {
    pub fn new(action: impl Into<String>) -> Self {
        Self {
            action: action.into(),
            reasoning: None,
            status: PlanStepStatus::Pending,
            result: None,
        }
    }

    pub fn with_reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.reasoning = Some(reasoning.into());
        self
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ActionPlan {
    pub steps: Vec<ActionPlanStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl ActionPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_step(&mut self, step: ActionPlanStep) {
        self.steps.push(step);
    }

    pub fn pending_steps(&self) -> Vec<&ActionPlanStep> {
        self.steps
            .iter()
            .filter(|s| s.status == PlanStepStatus::Pending)
            .collect()
    }

    pub fn next_step(&self) -> Option<&ActionPlanStep> {
        self.steps
            .iter()
            .find(|s| s.status == PlanStepStatus::Pending)
    }

    pub fn mark_completed(&mut self, index: usize, result: ActionResult) {
        if let Some(step) = self.steps.get_mut(index) {
            step.status = if result.success {
                PlanStepStatus::Completed
            } else {
                PlanStepStatus::Failed
            };
            step.result = Some(result);
        }
    }

    pub fn is_complete(&self) -> bool {
        self.steps.iter().all(|s| s.status != PlanStepStatus::Pending)
    }

    pub fn completed_count(&self) -> usize {
        self.steps
            .iter()
            .filter(|s| s.status == PlanStepStatus::Completed)
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.steps
            .iter()
            .filter(|s| s.status == PlanStepStatus::Failed)
            .count()
    }
}

pub const ACTION_PLANNER_TEMPLATE: &str = r#"# INSTRUCTIONS: Create an action plan for {{agentName}}.

Based on the conversation and available actions, determine what actions (if any) {{agentName}} should take.

Available actions:
{{actions}}

{{#if recentMessages}}Recent conversation:
{{recentMessages}}
{{/if}}

Current message: {{currentMessage}}

Respond with a JSON array of action steps. Each step should have:
- "action": the action name (must be from the available actions list)
- "reasoning": why this action should be taken

If no actions are needed, respond with an empty array: []

Example: [{"action": "SEARCH", "reasoning": "User asked for information that requires searching"}]

Respond ONLY with the JSON array, no other text."#;
