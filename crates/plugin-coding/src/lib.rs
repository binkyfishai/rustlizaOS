use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use regex::Regex;
use serde::Serialize;
use tokio::process::Command;
use tracing::{debug, info};

use rustliza_core::error::Result;
use rustliza_core::traits::{Action, Provider, ProviderResult, Runtime};
use rustliza_core::types::*;
use rustliza_core::{Character, Plugin};

// ---------------------------------------------------------------------------
// Coding events (streamed to callers)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum CodingEvent {
    Status(String),
    Thinking(String),
    Tool(String),
    Output(String),
    Done(String),
    Error(String),
    Workspace { branch: String, dir: String },
    FileChanged { path: String, action: String },
    Iteration(usize),
}

// ---------------------------------------------------------------------------
// Tool definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Tool {
    ReadFile { path: String },
    WriteFile { path: String, content: String },
    EditFile { path: String, old: String, new: String },
    Run { command: String },
    Search { pattern: String, path: Option<String> },
    Ls { path: String },
    Done { summary: String },
}

impl Tool {
    pub fn display(&self) -> String {
        match self {
            Tool::ReadFile { path } => format!("read_file: {}", path),
            Tool::WriteFile { path, .. } => format!("write_file: {}", path),
            Tool::EditFile { path, .. } => format!("edit_file: {}", path),
            Tool::Run { command } => format!("run: {}", command),
            Tool::Search { pattern, path } => {
                format!("search: {} in {}", pattern, path.as_deref().unwrap_or("."))
            }
            Tool::Ls { path } => format!("ls: {}", path),
            Tool::Done { summary } => format!("done: {}", summary),
        }
    }
}

// ---------------------------------------------------------------------------
// Tool parser — extracts tool calls from LLM response
// ---------------------------------------------------------------------------

static RE_READ: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<read_file>(.*?)</read_file>").unwrap());

static RE_WRITE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"<write_file\s+path="([^"]+)">([\s\S]*?)</write_file>"#).unwrap());

static RE_EDIT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"<edit_file\s+path="([^"]+)">\s*<old>([\s\S]*?)</old>\s*<new>([\s\S]*?)</new>\s*</edit_file>"#,
    )
    .unwrap()
});

static RE_RUN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<run>([\s\S]*?)</run>").unwrap());

static RE_SEARCH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"<search(?:\s+path="([^"]*)")?>([^<]*)</search>"#).unwrap());

static RE_LS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<ls>(.*?)</ls>").unwrap());

static RE_DONE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"<done>([\s\S]*?)</done>").unwrap());

pub fn parse_tools(response: &str) -> Vec<Tool> {
    let mut tools = vec![];

    for cap in RE_READ.captures_iter(response) {
        tools.push(Tool::ReadFile {
            path: cap[1].trim().to_string(),
        });
    }

    for cap in RE_WRITE.captures_iter(response) {
        tools.push(Tool::WriteFile {
            path: cap[1].trim().to_string(),
            content: cap[2].to_string(),
        });
    }

    for cap in RE_EDIT.captures_iter(response) {
        tools.push(Tool::EditFile {
            path: cap[1].trim().to_string(),
            old: cap[2].to_string(),
            new: cap[3].to_string(),
        });
    }

    for cap in RE_RUN.captures_iter(response) {
        tools.push(Tool::Run {
            command: cap[1].trim().to_string(),
        });
    }

    for cap in RE_SEARCH.captures_iter(response) {
        tools.push(Tool::Search {
            pattern: cap[2].trim().to_string(),
            path: cap.get(1).map(|m| m.as_str().to_string()),
        });
    }

    for cap in RE_LS.captures_iter(response) {
        tools.push(Tool::Ls {
            path: cap[1].trim().to_string(),
        });
    }

    for cap in RE_DONE.captures_iter(response) {
        tools.push(Tool::Done {
            summary: cap[1].trim().to_string(),
        });
    }

    tools
}

// ---------------------------------------------------------------------------
// Path safety
// ---------------------------------------------------------------------------

fn resolve_path(project_dir: &Path, relative: &str) -> std::result::Result<PathBuf, String> {
    let resolved = project_dir.join(relative);
    let canonical_base = project_dir
        .canonicalize()
        .map_err(|e| format!("bad project dir: {}", e))?;

    if resolved.exists() {
        let canonical = resolved
            .canonicalize()
            .map_err(|e| format!("bad path: {}", e))?;
        if !canonical.starts_with(&canonical_base) {
            return Err("path outside project directory".into());
        }
        Ok(canonical)
    } else {
        let parent = resolved.parent().ok_or("no parent directory")?;
        if parent.exists() {
            let canonical_parent = parent
                .canonicalize()
                .map_err(|e| format!("bad parent: {}", e))?;
            if !canonical_parent.starts_with(&canonical_base) {
                return Err("path outside project directory".into());
            }
        }
        Ok(resolved)
    }
}

// ---------------------------------------------------------------------------
// Tool executor
// ---------------------------------------------------------------------------

pub async fn execute_tool(project_dir: &Path, tool: &Tool) -> String {
    match tool {
        Tool::ReadFile { path } => match resolve_path(project_dir, path) {
            Ok(resolved) => match tokio::fs::read_to_string(&resolved).await {
                Ok(content) => content
                    .lines()
                    .enumerate()
                    .map(|(i, line)| format!("{:4} | {}", i + 1, line))
                    .collect::<Vec<_>>()
                    .join("\n"),
                Err(e) => format!("error reading {}: {}", path, e),
            },
            Err(e) => format!("error: {}", e),
        },

        Tool::WriteFile { path, content } => match resolve_path(project_dir, path) {
            Ok(resolved) => {
                if let Some(parent) = resolved.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                let trimmed = content.strip_prefix('\n').unwrap_or(content);
                match tokio::fs::write(&resolved, trimmed).await {
                    Ok(()) => format!("wrote {} ({} bytes)", path, trimmed.len()),
                    Err(e) => format!("error writing {}: {}", path, e),
                }
            }
            Err(e) => format!("error: {}", e),
        },

        Tool::EditFile { path, old, new } => match resolve_path(project_dir, path) {
            Ok(resolved) => match tokio::fs::read_to_string(&resolved).await {
                Ok(content) => {
                    let old_trimmed = old.trim();
                    let new_trimmed = new.trim();
                    if content.contains(old_trimmed) {
                        let updated = content.replacen(old_trimmed, new_trimmed, 1);
                        match tokio::fs::write(&resolved, &updated).await {
                            Ok(()) => format!(
                                "edited {} ({} → {} bytes replaced)",
                                path,
                                old_trimmed.len(),
                                new_trimmed.len()
                            ),
                            Err(e) => format!("error writing {}: {}", path, e),
                        }
                    } else {
                        format!("error: old text not found in {}", path)
                    }
                }
                Err(e) => format!("error reading {}: {}", path, e),
            },
            Err(e) => format!("error: {}", e),
        },

        Tool::Run { command } => run_shell(project_dir, command, 60).await,

        Tool::Search { pattern, path } => {
            let search_path = path.as_deref().unwrap_or(".");
            let escaped = pattern.replace('\'', "'\\''");
            let cmd = format!(
                "grep -rn --binary-files=without-match '{}' {} 2>/dev/null | head -50",
                escaped, search_path
            );
            run_shell(project_dir, &cmd, 15).await
        }

        Tool::Ls { path } => {
            let p = if path.is_empty() { "." } else { path.as_str() };
            let cmd = format!(
                "find {} -maxdepth 3 -not -path '*/target/*' -not -path '*/.git/*' -not -path '*/node_modules/*' 2>/dev/null | sort | head -100",
                p
            );
            run_shell(project_dir, &cmd, 10).await
        }

        Tool::Done { summary } => summary.clone(),
    }
}

async fn run_shell(dir: &Path, command: &str, timeout_secs: u64) -> String {
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(timeout_secs),
        Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(dir)
            .output(),
    )
    .await;

    match result {
        Ok(Ok(output)) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let mut out = String::new();
            if !stdout.is_empty() {
                out.push_str(&stdout);
            }
            if !stderr.is_empty() {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str("stderr: ");
                out.push_str(&stderr);
            }
            if out.is_empty() {
                format!("(exit {})", output.status.code().unwrap_or(-1))
            } else if out.len() > 10_000 {
                format!(
                    "{}...\n[truncated, {} total bytes]",
                    &out[..10_000],
                    out.len()
                )
            } else {
                out
            }
        }
        Ok(Err(e)) => format!("command failed: {}", e),
        Err(_) => format!("timed out after {}s", timeout_secs),
    }
}

// ---------------------------------------------------------------------------
// Project context
// ---------------------------------------------------------------------------

pub async fn get_project_context(project_dir: &Path) -> String {
    let mut ctx = String::new();

    let tree = run_shell(
        project_dir,
        "find . -maxdepth 3 -not -path '*/target/*' -not -path '*/.git/*' -not -path '*/node_modules/*' -not -name '*.o' -not -name '*.so' 2>/dev/null | sort | head -80",
        5,
    )
    .await;
    ctx.push_str("## Project files:\n```\n");
    ctx.push_str(&tree);
    ctx.push_str("\n```\n\n");

    let status = run_shell(
        project_dir,
        "git status --short 2>/dev/null || echo 'not a git repo'",
        5,
    )
    .await;
    ctx.push_str("## Git status:\n```\n");
    ctx.push_str(&status);
    ctx.push_str("\n```\n\n");

    let log = run_shell(
        project_dir,
        "git log --oneline -10 2>/dev/null || echo 'no git history'",
        5,
    )
    .await;
    ctx.push_str("## Recent commits:\n```\n");
    ctx.push_str(&log);
    ctx.push_str("\n```\n");

    ctx
}

// ---------------------------------------------------------------------------
// System prompt for coding loop
// ---------------------------------------------------------------------------

fn coding_system_prompt(character: &Character, project_dir: &Path) -> String {
    format!(
        r#"You are {name}, a senior autonomous coding engineer working on the project at `{dir}`.

You ship working code. You do not ask for permission, you do not ask clarifying questions, you do not stall. You research with tools, you build, you test, you commit.

You MUST use tools to accomplish tasks. Do NOT just describe what you would do — actually do it by writing the XML tool tags.

## Available tools

<read_file>path/to/file</read_file>
Read a file and see its contents with line numbers.

<write_file path="path/to/file">
file content here
</write_file>
Create or overwrite a file.

<edit_file path="path/to/file">
<old>exact text to find</old>
<new>replacement text</new>
</edit_file>
Replace the first occurrence of the old text with the new text in an existing file.

<run>shell command</run>
Run a shell command and see stdout/stderr.

<search>pattern</search>
<search path="src">pattern</search>
Grep for a pattern across source files.

<ls>path</ls>
List files in a directory (3 levels deep). Use "." for project root.

<done>summary of what you accomplished</done>
Signal that the current task is complete. Only use this AFTER you have made all changes.

## CRITICAL rules
- You MUST include at least one tool tag in every response. Never respond with only text.
- NEVER ask the user questions. Use your best judgment. If something is ambiguous, pick the most reasonable interpretation and build it.
- If you don't know something, research with <run>curl -s https://...</run> or <search>. Don't say "I need more info" — go get the info.
- Build a minimal working version FIRST, then iterate to improve it. Ship something that runs before perfecting.
- You are working on an isolated git branch. Commit your progress frequently with <run>git add -A && git commit -m "..."</run>. Commits are checkpoints, not finalizations.
- Always read a file before editing it.
- Use <edit_file> for surgical changes. Use <write_file> for new files or full rewrites.
- After making changes, RUN something to verify (build, tests, the program itself). Errors are signals, not failures — fix them.
- If you create a new sub-project, create a folder, scaffold the structure, write the code, then build it.
- You can use multiple tools in one response — chain them.
- Do NOT use <done> until you have a working artifact and have committed it.
- Follow the project's existing code style and conventions.

## Example response

Here is an example of a correct response when asked to add a README:

I'll start by looking at the project structure.

<ls>.</ls>

After seeing the file listing, a correct follow-up would be:

Now I'll create the README.

<write_file path="README.md">
# My Project

Description here.
</write_file>

<done>Created README.md with project description.</done>

{bio}"#,
        name = character.name,
        dir = project_dir.display(),
        bio = character.bio_text(),
    )
}

// ---------------------------------------------------------------------------
// Workspace management — auto-creates a git branch per task
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Workspace {
    pub project_dir: PathBuf,
    pub branch: String,
    pub original_branch: String,
    pub task_slug: String,
}

fn slugify(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|p| !p.is_empty())
        .take(4)
        .collect::<Vec<_>>()
        .join("-")
}

pub async fn create_workspace(project_dir: &Path, task: &str) -> Option<Workspace> {
    let is_git = run_shell(project_dir, "git rev-parse --git-dir 2>/dev/null", 5)
        .await
        .contains(".git");
    if !is_git {
        info!("not a git repo, skipping branch workspace");
        return None;
    }

    let original = run_shell(project_dir, "git branch --show-current 2>/dev/null", 5)
        .await
        .trim()
        .to_string();

    let slug = slugify(task);
    let stamp = chrono::Utc::now().format("%H%M%S");
    let branch = format!("botdick/{}-{}", slug, stamp);

    let result = run_shell(project_dir, &format!("git checkout -b {} 2>&1", branch), 10).await;
    if result.contains("Switched to a new branch") || result.contains("error") == false {
        info!(branch = %branch, "created workspace branch");
        Some(Workspace {
            project_dir: project_dir.to_path_buf(),
            branch,
            original_branch: original,
            task_slug: slug,
        })
    } else {
        info!(error = %result, "failed to create branch");
        None
    }
}

pub async fn changed_files(project_dir: &Path) -> Vec<(String, String)> {
    let out = run_shell(project_dir, "git status --porcelain 2>/dev/null", 5).await;
    out.lines()
        .filter_map(|line| {
            if line.len() < 4 {
                return None;
            }
            let status = line[..2].trim().to_string();
            let path = line[3..].to_string();
            let action = match status.as_str() {
                "M" | "MM" | " M" => "modified",
                "A" | "??" => "added",
                "D" => "deleted",
                "R" => "renamed",
                _ => "changed",
            };
            Some((path, action.to_string()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The autonomous coding loop
// ---------------------------------------------------------------------------

pub async fn run_coding_loop(
    runtime: &dyn Runtime,
    project_dir: &Path,
    task: &str,
    max_iterations: usize,
    tx: Option<tokio::sync::mpsc::Sender<CodingEvent>>,
) -> Result<String> {
    let character = runtime.character().clone();

    info!(task = %task, "coding loop started");
    emit(&tx, CodingEvent::Status(format!("starting: {}", task))).await;

    // Create isolated git branch workspace
    let workspace = create_workspace(project_dir, task).await;
    if let Some(ws) = &workspace {
        emit(
            &tx,
            CodingEvent::Workspace {
                branch: ws.branch.clone(),
                dir: ws.project_dir.display().to_string(),
            },
        )
        .await;
    }

    let system = coding_system_prompt(&character, project_dir);
    let mut messages: Vec<String> = vec![];
    let mut no_tool_streak = 0u32;
    let mut prev_changes: Vec<(String, String)> = vec![];

    let project_context = get_project_context(project_dir).await;
    let workspace_note = workspace
        .as_ref()
        .map(|ws| format!("Working on isolated branch `{}` (from `{}`).", ws.branch, ws.original_branch))
        .unwrap_or_else(|| "Not a git repository — working directly in the project dir.".into());
    messages.push(format!(
        "## Workspace:\n{}\n\n## Project context:\n{}\n\n## Task:\n{}",
        workspace_note, project_context, task
    ));

    for iteration in 0..max_iterations {
        let prompt = messages.join("\n\n---\n\n");

        emit(&tx, CodingEvent::Iteration(iteration + 1)).await;

        let response = runtime
            .generate_text(&GenerateTextParams {
                model_type: ModelType::TextLarge,
                system_prompt: system.clone(),
                prompt,
                max_tokens: Some(8192),
                temperature: Some(0.2),
                stop_sequences: vec![],
            })
            .await?;

        info!(iteration, len = response.len(), "LLM responded");
        emit(&tx, CodingEvent::Thinking(response.clone())).await;

        let tools = parse_tools(&response);

        if tools.is_empty() {
            no_tool_streak += 1;
            if no_tool_streak >= 3 {
                info!("no tools called 3 times in a row, ending loop");
                emit(&tx, CodingEvent::Done(response.clone())).await;
                return Ok(response);
            }
            messages.push(format!(
                "## Assistant:\n{}\n\n## System:\nYou did not use any tools. You MUST use tool tags to take action. Do not ask questions — make your best attempt. Start with <ls>.</ls> or <read_file> to explore, then build what was asked for.",
                response
            ));
            continue;
        }

        no_tool_streak = 0;

        let mut tool_log = format!(
            "## Assistant (iteration {}):\n{}\n\n## Tool results:\n",
            iteration + 1,
            response
        );

        for tool in &tools {
            if let Tool::Done { summary } = tool {
                info!(summary = %summary, "task complete");
                emit(&tx, CodingEvent::Done(summary.clone())).await;
                return Ok(summary.clone());
            }

            info!(tool = %tool.display(), "executing");
            emit(&tx, CodingEvent::Tool(tool.display())).await;

            let result = execute_tool(project_dir, tool).await;
            debug!(result_len = result.len(), "tool result");
            emit(&tx, CodingEvent::Output(truncate_for_event(&result, 2000))).await;

            tool_log.push_str(&format!(
                "### {}\n```\n{}\n```\n\n",
                tool.display(),
                result
            ));
        }

        messages.push(tool_log);

        // Detect file changes and emit FileChanged events for any new ones
        let now_changes = changed_files(project_dir).await;
        for (path, action) in &now_changes {
            if !prev_changes.iter().any(|(p, a)| p == path && a == action) {
                emit(
                    &tx,
                    CodingEvent::FileChanged {
                        path: path.clone(),
                        action: action.clone(),
                    },
                )
                .await;
            }
        }
        prev_changes = now_changes;

        // Trim context if too large — keep first (project context) + last 3 exchanges
        let total_len: usize = messages.iter().map(|m| m.len()).sum();
        if total_len > 50_000 && messages.len() > 3 {
            let first = messages[0].clone();
            let tail: Vec<String> = messages[messages.len() - 3..].to_vec();
            let tail_len = tail.len();
            messages = vec![first, "[earlier iterations trimmed]".into()];
            messages.extend(tail);
            debug!("trimmed context to {} entries (kept {})", messages.len(), tail_len);
        }
    }

    let msg = "reached maximum iterations — check project state".to_string();
    emit(&tx, CodingEvent::Done(msg.clone())).await;
    Ok(msg)
}

async fn emit(tx: &Option<tokio::sync::mpsc::Sender<CodingEvent>>, event: CodingEvent) {
    if let Some(tx) = tx {
        let _ = tx.send(event).await;
    }
}

fn truncate_for_event(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...[truncated]", &s[..max])
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Action: CODE — triggers the coding loop from the action system
// ---------------------------------------------------------------------------

pub struct CodeAction {
    project_dir: PathBuf,
}

#[async_trait]
impl Action for CodeAction {
    fn name(&self) -> &str {
        "CODE"
    }
    fn description(&self) -> &str {
        "Execute an autonomous coding task — reads, writes, and edits files, runs commands, iterates until done"
    }
    fn similes(&self) -> Vec<String> {
        vec![
            "IMPLEMENT".into(),
            "BUILD".into(),
            "FIX".into(),
            "REFACTOR".into(),
            "DEVELOP".into(),
        ]
    }
    fn priority(&self) -> i32 {
        10
    }

    async fn validate(
        &self,
        _runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<bool> {
        Ok(true)
    }

    async fn handler(
        &self,
        runtime: &dyn Runtime,
        message: &Memory,
        _state: &State,
    ) -> Result<ActionResult> {
        let task = message.content.text.clone().unwrap_or_default();
        if task.is_empty() {
            return Ok(ActionResult::err("no task description"));
        }

        match run_coding_loop(runtime, &self.project_dir, &task, 50, None).await {
            Ok(summary) => Ok(ActionResult::ok(summary)),
            Err(e) => Ok(ActionResult::err(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// Provider: project context injected into state
// ---------------------------------------------------------------------------

pub struct ProjectProvider {
    project_dir: PathBuf,
}

#[async_trait]
impl Provider for ProjectProvider {
    fn name(&self) -> &str {
        "project"
    }
    fn description(&self) -> &str {
        "Project file tree and git status"
    }

    async fn get(
        &self,
        _runtime: &dyn Runtime,
        _message: &Memory,
        _state: &State,
    ) -> Result<ProviderResult> {
        let ctx = get_project_context(&self.project_dir).await;
        Ok(ProviderResult {
            text: Some(format!("# Project: {}\n{}", self.project_dir.display(), ctx)),
            values: Default::default(),
            data: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Plugin constructor
// ---------------------------------------------------------------------------

pub fn coding_plugin(project_dir: PathBuf) -> Plugin {
    let mut plugin = Plugin::new(
        "coding",
        "Autonomous coding — file ops, shell, git, search, iterative task loop",
    );
    plugin
        .actions
        .push(Arc::new(CodeAction { project_dir: project_dir.clone() }));
    plugin
        .providers
        .push(Arc::new(ProjectProvider { project_dir }));
    plugin
}
