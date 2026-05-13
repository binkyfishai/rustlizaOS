use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;

use async_trait::async_trait;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::character::Character;
use crate::error::Result;
use crate::hooks::{HookAction, HookPhase, HookRegistry, PipelineHook};
use crate::metrics::{PipelineMetrics, Timer};
use crate::planner::{ActionPlan, ActionPlanStep};
use crate::summarize;
use crate::template;
use crate::traits::*;
use crate::types::*;

static ACTION_REGEX: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"\[([A-Z_]+)\]").unwrap());

// ---------------------------------------------------------------------------
// Prompt templates
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT_TEMPLATE: &str = r#"You are {{agentName}}.
{{#if system}}{{system}}{{/if}}

# About {{agentName}}:
{{bio}}

{{#if lore}}# Backstory:
{{lore}}
{{/if}}
{{#if knowledge}}# Knowledge:
{{knowledge}}
{{/if}}
{{#if topics}}# Topics of interest: {{topics}}{{/if}}
{{#if adjectives}}# Personality traits: {{adjectives}}{{/if}}

{{#if styleAll}}# Communication style:
{{styleAll}}
{{/if}}
{{#if styleChat}}# Chat style:
{{styleChat}}
{{/if}}
{{#if messageExamples}}# Example conversations:
{{messageExamples}}
{{/if}}"#;

const USER_PROMPT_TEMPLATE: &str = r#"{{#if providers}}# Context:
{{providers}}
{{/if}}
{{#if recentMessages}}# Recent conversation:
{{recentMessages}}
{{/if}}
{{#if actions}}# Available actions:
{{actions}}

When you want to take an action, include it in your response like: [ACTION_NAME]
Available actions: {{actionNames}}
{{/if}}
# Current message from {{senderName}}:
{{currentMessage}}

Respond as {{agentName}}.{{#if messageDirections}} {{messageDirections}}{{/if}}"#;

const SHOULD_RESPOND_TEMPLATE: &str = r#"# INSTRUCTIONS: Determine if {{agentName}} should respond to the message.

{{#if bio}}About {{agentName}}:
{{bio}}
{{/if}}

{{#if recentMessages}}# Recent conversation:
{{recentMessages}}
{{/if}}

# Current message from {{senderName}}:
{{currentMessage}}

Should {{agentName}} respond to this message? Consider:
- Is the message directed at {{agentName}}?
- Is it a direct message (always respond)?
- Would a response be natural and helpful?

Respond with ONLY one of: [RESPOND], [IGNORE], [STOP]"#;

// ---------------------------------------------------------------------------
// AgentRuntime
// ---------------------------------------------------------------------------

pub struct AgentRuntime {
    agent_id: AgentId,
    character: Character,
    database: Arc<dyn DatabaseAdapter>,
    model_provider: Arc<dyn ModelProvider>,
    actions: Vec<Arc<dyn Action>>,
    evaluators: Vec<Arc<dyn Evaluator>>,
    providers: Vec<Arc<dyn Provider>>,
    services: Vec<Arc<dyn Service>>,
    hooks: HookRegistry,
    action_planning_enabled: bool,
    metrics: Arc<PipelineMetrics>,
}

impl AgentRuntime {
    pub fn builder() -> AgentRuntimeBuilder {
        AgentRuntimeBuilder::default()
    }

    pub fn services(&self) -> &[Arc<dyn Service>] {
        &self.services
    }

    pub fn hooks(&self) -> &HookRegistry {
        &self.hooks
    }

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    pub async fn start_services(self: &Arc<Self>) -> Result<()> {
        for service in &self.services {
            info!(service = service.name(), "starting service");
            let rt: Arc<dyn Runtime> = self.clone();
            service.start(rt).await?;
        }
        Ok(())
    }

    pub async fn stop_services(&self) -> Result<()> {
        for service in &self.services {
            info!(service = service.name(), "stopping service");
            if let Err(e) = service.stop().await {
                warn!(service = service.name(), error = %e, "error stopping service");
            }
        }
        Ok(())
    }

    async fn compose_state_inner(&self, message: &Memory) -> Result<State> {
        let mut state = State::default();

        // Core character values
        state.values.insert("agentName".into(), self.character.name.clone());
        state.values.insert("bio".into(), self.character.bio_text());
        state.values.insert("lore".into(), self.character.lore_text());
        state.values.insert("topics".into(), self.character.topics_text());
        state.values.insert("adjectives".into(), self.character.adjectives_text());
        state.values.insert("styleAll".into(), self.character.style_all_text());
        state.values.insert("styleChat".into(), self.character.style_chat_text());
        state.values.insert(
            "messageExamples".into(),
            self.character.format_message_examples(),
        );
        state.values.insert("knowledge".into(), self.character.knowledge.join("\n"));

        if let Some(system) = &self.character.system {
            state.values.insert("system".into(), system.clone());
        }

        // Current message
        let current_text = message.content.text.as_deref().unwrap_or("");
        state.values.insert("currentMessage".into(), current_text.to_string());

        // Fetch entity, room, and recent messages concurrently
        let entity_fut = self.database.get_entity(message.entity_id);
        let room_fut = self.database.get_room(message.room_id);
        let recent_params = GetMemoriesParams {
            room_id: Some(message.room_id),
            agent_id: Some(self.agent_id),
            memory_type: Some(MemoryType::Message),
            count: Some(20),
            ..Default::default()
        };
        let recent_fut = self.database.get_memories(&recent_params);

        let (entity_res, room_res, recent_res) =
            futures::future::join3(entity_fut, room_fut, recent_fut).await;

        if let Ok(Some(entity)) = entity_res {
            state.values.insert("senderName".into(), entity.display_name().to_string());
            state.data.entity = Some(entity);
        } else {
            state.values.insert("senderName".into(), "User".into());
        }

        if let Ok(Some(room)) = room_res {
            state.data.room = Some(room);
        }

        let recent = recent_res.unwrap_or_default();

        let recent_text = if summarize::should_summarize(&recent, 15) {
            let (old, kept) = summarize::split_for_summary(&recent, 8);
            match summarize::summarize_messages(old, &self.character.name, self.model_provider.as_ref()).await {
                Ok(summary) => {
                    summarize::format_with_summary(&summary, kept, &self.character.name)
                }
                Err(_) => format_recent_messages(&recent, &self.character.name),
            }
        } else {
            format_recent_messages(&recent, &self.character.name)
        };

        state.values.insert("recentMessages".into(), recent_text);
        state.data.recent_messages = Some(recent);

        // Actions
        let action_descriptions: Vec<String> = self
            .actions
            .iter()
            .map(|a| format!("- {}: {}", a.name(), a.description()))
            .collect();
        state.values.insert("actions".into(), action_descriptions.join("\n"));
        let action_names: Vec<String> = self.actions.iter().map(|a| a.name().to_string()).collect();
        state.values.insert("actionNames".into(), action_names.join(", "));

        // Run providers in parallel
        let public_providers: Vec<&Arc<dyn Provider>> = self
            .providers
            .iter()
            .filter(|p| !p.is_private())
            .collect();

        let provider_futs: Vec<_> = public_providers
            .iter()
            .map(|p| {
                let name = p.name().to_string();
                let fut = p.get(self, message, &state);
                async move { (name, fut.await) }
            })
            .collect();

        let results = futures::future::join_all(provider_futs).await;

        let mut provider_texts = Vec::new();
        for (name, result) in results {
            match result {
                Ok(pr) => {
                    if let Some(text) = pr.text {
                        provider_texts.push(format!("[{}]\n{}", name, text));
                    }
                    for (k, v) in pr.values {
                        state.values.insert(k, v);
                    }
                }
                Err(e) => {
                    warn!(provider = %name, error = %e, "provider error");
                }
            }
        }
        state.values.insert("providers".into(), provider_texts.join("\n\n"));

        Ok(state)
    }

    async fn should_respond(&self, message: &Memory, state: &State) -> Result<bool> {
        // Always respond to DMs
        if let Some(room) = &state.data.room {
            if room.channel_type == ChannelType::Dm {
                return Ok(true);
            }
        }

        // Check if mentioned by name
        if let Some(text) = &message.content.text {
            let name_lower = self.character.name.to_lowercase();
            if text.to_lowercase().contains(&name_lower) {
                return Ok(true);
            }
        }

        // Ask the model
        let prompt = template::render(SHOULD_RESPOND_TEMPLATE, &state.values);
        let response = self
            .model_provider
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextSmall,
                system_prompt: format!(
                    "You are a response decision engine for {}. Respond with ONLY [RESPOND], [IGNORE], or [STOP].",
                    self.character.name
                ),
                prompt,
                max_tokens: Some(10),
                temperature: Some(0.3),
                stop_sequences: vec![],
            })
            .await?;

        let decision = response.to_uppercase();
        Ok(decision.contains("RESPOND"))
    }

    async fn generate_response_stream(
        &self,
        _message: &Memory,
        state: &State,
    ) -> Result<TextStream> {
        let system_prompt = template::render(SYSTEM_PROMPT_TEMPLATE, &state.values);
        let user_prompt = template::render(USER_PROMPT_TEMPLATE, &state.values);

        self.model_provider
            .generate_text_stream(&GenerateTextParams {
                model_type: ModelType::TextLarge,
                system_prompt,
                prompt: user_prompt,
                max_tokens: Some(2048),
                temperature: Some(0.7),
                stop_sequences: vec![],
            })
            .await
    }

    async fn generate_response(&self, _message: &Memory, state: &State) -> Result<Content> {
        let system_prompt = template::render(SYSTEM_PROMPT_TEMPLATE, &state.values);
        let user_prompt = template::render(USER_PROMPT_TEMPLATE, &state.values);

        debug!(
            system_len = system_prompt.len(),
            user_len = user_prompt.len(),
            "generating response"
        );

        let response_text = self
            .model_provider
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextLarge,
                system_prompt,
                prompt: user_prompt,
                max_tokens: Some(2048),
                temperature: Some(0.7),
                stop_sequences: vec![],
            })
            .await?;

        let (text, actions) = extract_actions(&response_text);

        Ok(Content {
            text: Some(text),
            actions: if actions.is_empty() { None } else { Some(actions) },
            ..Default::default()
        })
    }

    async fn run_evaluators(&self, message: &Memory, state: &State) -> Result<()> {
        for evaluator in &self.evaluators {
            let should_run = if evaluator.always_run() {
                true
            } else {
                evaluator.validate(self, message, state).await.unwrap_or(false)
            };

            if should_run {
                debug!(evaluator = evaluator.name(), "running evaluator");
                if let Err(e) = evaluator.handler(self, message, state).await {
                    warn!(evaluator = evaluator.name(), error = %e, "evaluator error");
                }
            }
        }
        Ok(())
    }

    async fn process_actions_inner(
        &self,
        action_names: &[String],
        message: &Memory,
        state: &State,
    ) -> Result<Vec<ActionResult>> {
        let mut results = Vec::new();

        for action_name in action_names {
            let action = self.actions.iter().find(|a| {
                a.name().eq_ignore_ascii_case(action_name)
                    || a.similes().iter().any(|s| s.eq_ignore_ascii_case(action_name))
            });

            if let Some(action) = action {
                let valid = action.validate(self, message, state).await.unwrap_or(false);
                if valid {
                    debug!(action = action.name(), "executing action");
                    match action.handler(self, message, state).await {
                        Ok(result) => {
                            let should_continue = result.continue_chain;
                            results.push(result);
                            if !should_continue {
                                break;
                            }
                        }
                        Err(e) => {
                            warn!(action = action.name(), error = %e, "action error");
                            results.push(ActionResult::err(e.to_string()));
                        }
                    }
                }
            } else {
                debug!(action = action_name, "action not found, skipping");
            }
        }

        Ok(results)
    }

    async fn generate_action_plan(&self, message: &Memory, state: &State) -> Result<ActionPlan> {
        let prompt = template::render(crate::planner::ACTION_PLANNER_TEMPLATE, &state.values);

        let response = self
            .model_provider
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextSmall,
                system_prompt: "You are an action planning engine. Respond ONLY with a JSON array of action steps.".into(),
                prompt,
                max_tokens: Some(512),
                temperature: Some(0.3),
                stop_sequences: vec![],
            })
            .await?;

        // Parse the JSON response
        let trimmed = response.trim();
        let json_str = if let Some(start) = trimmed.find('[') {
            if let Some(end) = trimmed.rfind(']') {
                &trimmed[start..=end]
            } else {
                "[]"
            }
        } else {
            "[]"
        };

        #[derive(serde::Deserialize)]
        struct PlanEntry {
            action: String,
            reasoning: Option<String>,
        }

        let entries: Vec<PlanEntry> = serde_json::from_str(json_str).unwrap_or_default();
        let mut plan = ActionPlan::new();

        for entry in entries {
            // Only include actions that actually exist
            let exists = self.actions.iter().any(|a| {
                a.name().eq_ignore_ascii_case(&entry.action)
                    || a.similes().iter().any(|s| s.eq_ignore_ascii_case(&entry.action))
            });
            if exists {
                let mut step = ActionPlanStep::new(&entry.action);
                if let Some(r) = entry.reasoning {
                    step = step.with_reasoning(r);
                }
                plan.add_step(step);
            }
        }

        debug!(steps = plan.steps.len(), "generated action plan");
        let _ = message; // used for context in prompt
        Ok(plan)
    }

    async fn execute_action_plan(
        &self,
        plan: &mut ActionPlan,
        message: &Memory,
        state: &State,
    ) -> Result<Vec<ActionResult>> {
        let mut results = Vec::new();

        for i in 0..plan.steps.len() {
            let action_name = plan.steps[i].action.clone();

            // Run pre-action hook
            if let Ok(HookAction::Skip | HookAction::Abort) =
                self.hooks.run(HookPhase::PreAction, message, state).await
            {
                break;
            }

            let action_results = self.process_actions_inner(&[action_name], message, state).await?;

            let result = action_results.into_iter().next().unwrap_or(ActionResult::ok("completed"));
            let should_continue = result.continue_chain;
            plan.mark_completed(i, result.clone());
            results.push(result);

            // Run post-action hook
            let _ = self.hooks.run(HookPhase::PostAction, message, state).await;

            if !should_continue {
                break;
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Runtime for AgentRuntime {
    fn agent_id(&self) -> AgentId {
        self.agent_id
    }

    fn character(&self) -> &Character {
        &self.character
    }

    fn database(&self) -> &dyn DatabaseAdapter {
        self.database.as_ref()
    }

    fn model_provider(&self) -> &dyn ModelProvider {
        self.model_provider.as_ref()
    }

    fn actions(&self) -> &[Arc<dyn Action>] {
        &self.actions
    }

    fn evaluators(&self) -> &[Arc<dyn Evaluator>] {
        &self.evaluators
    }

    fn providers(&self) -> &[Arc<dyn Provider>] {
        &self.providers
    }

    async fn compose_state(&self, message: &Memory) -> Result<State> {
        self.compose_state_inner(message).await
    }

    async fn generate_text(&self, params: &GenerateTextParams) -> Result<String> {
        self.model_provider.generate_text(params).await
    }

    async fn generate_text_stream(&self, params: &GenerateTextParams) -> Result<TextStream> {
        self.model_provider.generate_text_stream(params).await
    }

    async fn generate_embedding(&self, text: &str) -> Result<Vec<f32>> {
        self.model_provider.generate_embedding(text).await
    }

    async fn process_message_stream(
        &self,
        message: &Memory,
    ) -> Result<Pin<Box<dyn futures::Stream<Item = StreamEvent> + Send>>> {
        info!(sender = %message.entity_id, room = %message.room_id, "processing message (streaming)");

        if let Ok(HookAction::Abort) = self
            .hooks
            .run(HookPhase::IncomingBeforeCompose, message, &State::default())
            .await
        {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let _ = tx.send(StreamEvent::Done { full_text: String::new() }).await;
            return Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)));
        }

        self.database.create_memory(message).await?;
        let state = self.compose_state_inner(message).await?;

        if let Ok(HookAction::Abort) = self.hooks.run(HookPhase::PreShouldRespond, message, &state).await {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let _ = tx.send(StreamEvent::Done { full_text: String::new() }).await;
            return Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)));
        }

        let should = self.should_respond(message, &state).await?;
        let _ = self.hooks.run(HookPhase::PostShouldRespond, message, &state).await;

        if !should {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let _ = tx.send(StreamEvent::Done { full_text: String::new() }).await;
            return Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)));
        }

        if let Ok(HookAction::Abort) = self.hooks.run(HookPhase::PreGenerate, message, &state).await {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let _ = tx.send(StreamEvent::Done { full_text: String::new() }).await;
            return Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)));
        }

        let mut inner_rx = self.generate_response_stream(message, &state).await?;

        // Wrap the inner stream: on Done, store the response memory and run post-processing
        let (outer_tx, outer_rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
        let agent_id = self.agent_id;
        let room_id = message.room_id;
        let db = self.database.clone();
        let mp = self.model_provider.clone();

        tokio::spawn(async move {
            while let Some(event) = inner_rx.recv().await {
                match &event {
                    StreamEvent::Token(_) => {
                        let _ = outer_tx.send(event).await;
                    }
                    StreamEvent::Done { full_text } => {
                        let (clean_text, _actions) = extract_actions(full_text);
                        let content = Content {
                            text: Some(clean_text.clone()),
                            ..Default::default()
                        };
                        let response_memory = Memory::new_message(
                            agent_id, agent_id, room_id, content,
                        );
                        let _ = db.create_memory(&response_memory).await;

                        // Background embedding
                        if !clean_text.is_empty() {
                            let mp2 = mp.clone();
                            let db2 = db.clone();
                            let mem = response_memory.clone();
                            tokio::spawn(async move {
                                if let Ok(emb) = mp2.generate_embedding(&mem.content.text.as_deref().unwrap_or("")).await {
                                    let mut m = mem;
                                    m.embedding = Some(emb);
                                    let _ = db2.create_memory(&m).await;
                                }
                            });
                        }

                        let _ = outer_tx.send(StreamEvent::Done { full_text: clean_text }).await;
                        return;
                    }
                    StreamEvent::Error(_) => {
                        let _ = outer_tx.send(event).await;
                        return;
                    }
                }
            }
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(outer_rx)))
    }

    async fn process_message(&self, message: &Memory) -> Result<Vec<Memory>> {
        let total_timer = Timer::start();
        info!(sender = %message.entity_id, room = %message.room_id, "processing message");

        // Hook: incoming before compose
        if let Ok(HookAction::Abort) = self
            .hooks
            .run(HookPhase::IncomingBeforeCompose, message, &State::default())
            .await
        {
            return Ok(vec![]);
        }

        // 1+2. Store incoming message and compose state concurrently
        let compose_timer = Timer::start();
        let (store_result, state) = futures::future::join(
            self.database.create_memory(message),
            self.compose_state_inner(message),
        )
        .await;
        store_result?;
        let state = state?;
        self.metrics.record_compose(compose_timer.elapsed_us());

        // Hook: pre should_respond
        if let Ok(HookAction::Abort) = self.hooks.run(HookPhase::PreShouldRespond, message, &state).await {
            return Ok(vec![]);
        }

        // 3. Decide whether to respond
        let respond_timer = Timer::start();
        let should = self.should_respond(message, &state).await?;
        self.metrics.record_should_respond(respond_timer.elapsed_us());

        // Hook: post should_respond
        let _ = self.hooks.run(HookPhase::PostShouldRespond, message, &state).await;

        if !should {
            self.metrics.record_skip();
            debug!("decided not to respond");
            return Ok(vec![]);
        }

        // Hook: pre generate
        if let Ok(HookAction::Abort) = self.hooks.run(HookPhase::PreGenerate, message, &state).await {
            return Ok(vec![]);
        }

        // 4. Generate response
        let gen_timer = Timer::start();
        let response_content = self.generate_response(message, &state).await?;
        self.metrics.record_generate(gen_timer.elapsed_us());

        // Hook: post generate
        let _ = self.hooks.run(HookPhase::PostGenerate, message, &state).await;

        // 5. Create response memory
        let response_memory = Memory::new_message(
            self.agent_id,
            self.agent_id,
            message.room_id,
            response_content.clone(),
        );
        self.database.create_memory(&response_memory).await?;

        // 6. Generate and store embedding in background (non-blocking)
        if let Some(text) = &response_content.text {
            if !text.is_empty() {
                let mp = self.model_provider.clone();
                let db = self.database.clone();
                let mem = response_memory.clone();
                let embed_text = text.clone();
                tokio::spawn(async move {
                    match mp.generate_embedding(&embed_text).await {
                        Ok(embedding) => {
                            let mut mem_with_embedding = mem;
                            mem_with_embedding.embedding = Some(embedding);
                            let _ = db.create_memory(&mem_with_embedding).await;
                        }
                        Err(e) => {
                            debug!(error = %e, "background embedding failed");
                        }
                    }
                });
            }
        }

        // Hook: pre evaluate
        let _ = self.hooks.run(HookPhase::PreEvaluate, message, &state).await;

        // 7. Run evaluators
        self.run_evaluators(message, &state).await?;

        // Hook: post evaluate
        let _ = self.hooks.run(HookPhase::PostEvaluate, message, &state).await;

        // 8. Process actions — either via planner or inline
        if self.action_planning_enabled && !self.actions.is_empty() {
            match self.generate_action_plan(message, &state).await {
                Ok(mut plan) => {
                    if !plan.steps.is_empty() {
                        let _ = self.execute_action_plan(&mut plan, message, &state).await;
                    }
                }
                Err(e) => {
                    debug!(error = %e, "action planning failed, falling back to inline");
                    if let Some(actions) = &response_content.actions {
                        if !actions.is_empty() {
                            let _ = self.process_actions_inner(actions, message, &state).await;
                        }
                    }
                }
            }
        } else if let Some(actions) = &response_content.actions {
            if !actions.is_empty() {
                let _ = self.process_actions_inner(actions, message, &state).await;
            }
        }

        self.metrics.record_message();
        self.metrics.record_total(total_timer.elapsed_us());
        info!(
            elapsed_ms = total_timer.elapsed_us() / 1000,
            "message processing complete"
        );
        Ok(vec![response_memory])
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct AgentRuntimeBuilder {
    agent_id: Option<AgentId>,
    character: Option<Character>,
    database: Option<Arc<dyn DatabaseAdapter>>,
    model_provider: Option<Arc<dyn ModelProvider>>,
    plugins: Vec<Plugin>,
    actions: Vec<Arc<dyn Action>>,
    evaluators: Vec<Arc<dyn Evaluator>>,
    providers: Vec<Arc<dyn Provider>>,
    services: Vec<Arc<dyn Service>>,
    hooks: Vec<PipelineHook>,
    action_planning: bool,
}

impl AgentRuntimeBuilder {
    pub fn agent_id(mut self, id: AgentId) -> Self {
        self.agent_id = Some(id);
        self
    }

    pub fn character(mut self, character: Character) -> Self {
        self.character = Some(character);
        self
    }

    pub fn database(mut self, db: Arc<dyn DatabaseAdapter>) -> Self {
        self.database = Some(db);
        self
    }

    pub fn model_provider(mut self, provider: Arc<dyn ModelProvider>) -> Self {
        self.model_provider = Some(provider);
        self
    }

    pub fn plugin(mut self, plugin: Plugin) -> Self {
        self.plugins.push(plugin);
        self
    }

    pub fn action(mut self, action: Arc<dyn Action>) -> Self {
        self.actions.push(action);
        self
    }

    pub fn evaluator(mut self, evaluator: Arc<dyn Evaluator>) -> Self {
        self.evaluators.push(evaluator);
        self
    }

    pub fn provider(mut self, provider: Arc<dyn Provider>) -> Self {
        self.providers.push(provider);
        self
    }

    pub fn service(mut self, service: Arc<dyn Service>) -> Self {
        self.services.push(service);
        self
    }

    pub fn hook(mut self, hook: PipelineHook) -> Self {
        self.hooks.push(hook);
        self
    }

    pub fn enable_action_planning(mut self) -> Self {
        self.action_planning = true;
        self
    }

    pub fn build(mut self) -> std::result::Result<Arc<AgentRuntime>, String> {
        // Merge plugins
        for plugin in self.plugins {
            self.actions.extend(plugin.actions);
            self.evaluators.extend(plugin.evaluators);
            self.providers.extend(plugin.providers);
            self.services.extend(plugin.services);
        }

        let mut hook_registry = HookRegistry::new();
        for hook in self.hooks {
            hook_registry.register(hook);
        }

        let runtime = AgentRuntime {
            agent_id: self.agent_id.unwrap_or_else(Uuid::new_v4),
            character: self.character.unwrap_or_default(),
            database: self.database.ok_or("database adapter required")?,
            model_provider: self.model_provider.ok_or("model provider required")?,
            actions: self.actions,
            evaluators: self.evaluators,
            providers: self.providers,
            services: self.services,
            hooks: hook_registry,
            action_planning_enabled: self.action_planning,
            metrics: PipelineMetrics::new(),
        };

        Ok(Arc::new(runtime))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_recent_messages(messages: &[Memory], agent_name: &str) -> String {
    let mut lines = Vec::new();
    for mem in messages {
        let sender = if mem.entity_id == mem.agent_id {
            agent_name.to_string()
        } else {
            format!("User({})", &mem.entity_id.to_string()[..8])
        };
        let text = mem.content.text.as_deref().unwrap_or("[no text]");
        lines.push(format!("{}: {}", sender, text));
    }
    lines.join("\n")
}

fn extract_actions(response: &str) -> (String, Vec<String>) {
    let re = &*ACTION_REGEX;
    let mut actions = Vec::new();

    for cap in re.captures_iter(response) {
        let action = cap[1].to_string();
        if action != "RESPOND" && action != "IGNORE" && action != "STOP" {
            actions.push(action);
        }
    }

    let clean_text = re.replace_all(response, "").trim().to_string();
    (clean_text, actions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_actions() {
        let (text, actions) =
            extract_actions("I'll help with that! [SEARCH] Let me find it for you.");
        assert_eq!(actions, vec!["SEARCH"]);
        assert!(!text.contains("[SEARCH]"));
    }

    #[test]
    fn test_extract_no_actions() {
        let (text, actions) = extract_actions("Just a normal response.");
        assert!(actions.is_empty());
        assert_eq!(text, "Just a normal response.");
    }

    #[test]
    fn test_format_recent_messages() {
        let agent_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        let room_id = Uuid::new_v4();

        let messages = vec![
            Memory::new_message(agent_id, user_id, room_id, Content::text("Hello")),
            Memory::new_message(agent_id, agent_id, room_id, Content::text("Hi there!")),
        ];

        let formatted = format_recent_messages(&messages, "Eliza");
        assert!(formatted.contains("Hello"));
        assert!(formatted.contains("Eliza: Hi there!"));
    }
}
