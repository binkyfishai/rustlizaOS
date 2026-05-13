use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use futures::StreamExt;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tracing::{error, info};
use uuid::Uuid;

use rustliza_core::traits::{DatabaseAdapter, ModelProvider, Runtime};
use rustliza_core::types::*;
use rustliza_core::{AgentRuntime, Character};
use rustliza_client_discord::DiscordService;
use rustliza_client_telegram::TelegramService;
use rustliza_client_twitter::TwitterService;
use rustliza_db_sqlite::SqliteAdapter;
use rustliza_knowledge::KnowledgeProvider;
use rustliza_model_anthropic::AnthropicProvider;
use rustliza_model_openai::OpenAIProvider;
use rustliza_plugin_bootstrap::bootstrap_plugin;

#[derive(Parser)]
#[command(name = "rustliza", about = "Rustliza — ElizaOS rewritten in Rust")]
struct Cli {
    /// Path to character JSON file
    #[arg(short, long, default_value = "characters/default.json")]
    character: PathBuf,

    /// Path to SQLite database file
    #[arg(short, long, default_value = "rustliza.db")]
    database: PathBuf,

    /// Anthropic API key (or set ANTHROPIC_API_KEY env var)
    #[arg(long, env = "ANTHROPIC_API_KEY")]
    api_key: Option<String>,

    /// Small model override
    #[arg(long)]
    small_model: Option<String>,

    /// Large model override
    #[arg(long)]
    large_model: Option<String>,

    /// Start the HTTP API server
    #[arg(long)]
    api: bool,

    /// API server bind address
    #[arg(long, default_value = "0.0.0.0:3000")]
    api_bind: String,

    /// Discord bot token (or set DISCORD_TOKEN env var)
    #[arg(long, env = "DISCORD_TOKEN")]
    discord_token: Option<String>,

    /// Telegram bot token (or set TELEGRAM_BOT_TOKEN env var)
    #[arg(long, env = "TELEGRAM_BOT_TOKEN")]
    telegram_token: Option<String>,

    /// Twitter/X bearer token (or set TWITTER_BEARER_TOKEN env var)
    #[arg(long, env = "TWITTER_BEARER_TOKEN")]
    twitter_token: Option<String>,

    /// Use OpenAI-compatible provider instead of Anthropic
    #[arg(long)]
    openai: bool,

    /// OpenAI-compatible base URL (default: https://api.openai.com/v1)
    #[arg(long)]
    openai_base_url: Option<String>,

    /// OpenAI API key (or set OPENAI_API_KEY env var)
    #[arg(long, env = "OPENAI_API_KEY")]
    openai_api_key: Option<String>,

    /// Enable multi-step action planning
    #[arg(long)]
    action_planning: bool,

    /// Ingest a file into the knowledge base before starting
    #[arg(long)]
    ingest: Option<PathBuf>,

    /// Run headless (no REPL — just services)
    #[arg(long)]
    headless: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    // Load character
    let character = if cli.character.exists() {
        Character::load(&cli.character)
            .with_context(|| format!("failed to load character from {:?}", cli.character))?
    } else {
        info!("character file not found, using default");
        Character::default()
    };

    info!(name = %character.name, "loaded character");

    // Model provider
    let model_provider: Arc<dyn ModelProvider> = if cli.openai {
        let api_key = cli
            .openai_api_key
            .or_else(|| std::env::var("OPENAI_API_KEY").ok())
            .or_else(|| character.settings.secrets.get("OPENAI_API_KEY").cloned())
            .context("OPENAI_API_KEY not set — pass --openai-api-key or set the env var")?;

        let mut provider = OpenAIProvider::new(&api_key);
        if let Some(url) = &cli.openai_base_url {
            provider = provider.with_base_url(url);
        }
        if let (Some(sm), Some(lm)) = (&cli.small_model, &cli.large_model) {
            provider = provider.with_models(sm, lm);
        }
        info!("using OpenAI-compatible provider");
        Arc::new(provider)
    } else {
        let api_key = cli
            .api_key
            .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
            .or_else(|| character.settings.secrets.get("ANTHROPIC_API_KEY").cloned())
            .context("ANTHROPIC_API_KEY not set — pass --api-key or set the env var")?;

        let provider = if let (Some(sm), Some(lm)) = (&cli.small_model, &cli.large_model) {
            AnthropicProvider::with_models(&api_key, sm, lm)
        } else {
            AnthropicProvider::new(&api_key)
        };
        Arc::new(provider)
    };

    // Database
    let db = Arc::new(SqliteAdapter::new(&cli.database).await?);
    db.init().await?;
    info!(path = ?cli.database, "database ready");

    // Build runtime
    let agent_id = Uuid::new_v4();
    let mut builder = AgentRuntime::builder()
        .agent_id(agent_id)
        .character(character.clone())
        .database(db.clone() as Arc<dyn rustliza_core::DatabaseAdapter>)
        .model_provider(model_provider.clone())
        .plugin(bootstrap_plugin())
        .provider(Arc::new(KnowledgeProvider::new(
            db.clone() as Arc<dyn rustliza_core::DatabaseAdapter>,
            model_provider.clone(),
        )));

    if cli.action_planning {
        builder = builder.enable_action_planning();
    }

    // Add Discord service if token provided
    if let Some(token) = &cli.discord_token {
        builder = builder.service(Arc::new(DiscordService::new(token)));
    }

    // Add Telegram service if token provided
    if let Some(token) = &cli.telegram_token {
        builder = builder.service(Arc::new(TelegramService::new(token)));
    }

    // Add Twitter service if token provided
    if let Some(token) = &cli.twitter_token {
        builder = builder.service(Arc::new(TwitterService::new(token)));
    }

    let runtime = builder.build().map_err(|e| anyhow::anyhow!(e))?;

    // Ingest knowledge file if specified
    if let Some(path) = &cli.ingest {
        info!(path = ?path, "ingesting knowledge file");
        let room_id = Uuid::new_v4();
        let pipeline = rustliza_knowledge::KnowledgePipeline::new(
            db.clone() as Arc<dyn rustliza_core::DatabaseAdapter>,
            model_provider.clone(),
        );
        let fragments = pipeline.ingest_file(path, agent_id, room_id).await?;
        info!(fragments = fragments.len(), "knowledge ingestion complete");
    }

    // Start services (Discord, Telegram, etc.)
    if !runtime.services().is_empty() {
        runtime.start_services().await?;
    }

    // Start API server if requested
    if cli.api {
        let rt = runtime.clone() as Arc<dyn Runtime>;
        let bind = cli.api_bind.clone();
        tokio::spawn(async move {
            if let Err(e) = rustliza_server_api::start_server(rt, &bind).await {
                error!(error = %e, "API server error");
            }
        });
        info!(bind = %cli.api_bind, "API server started");
    }

    if cli.headless {
        info!("running headless — press Ctrl-C to stop");
        tokio::signal::ctrl_c().await?;
        runtime.stop_services().await?;
        return Ok(());
    }

    // Set up the CLI user and room
    let user_id = Uuid::new_v4();
    let room_id = Uuid::new_v4();

    let user_entity = rustliza_core::Entity {
        id: user_id,
        agent_id,
        names: vec!["User".to_string()],
        metadata: None,
        created_at: Some(chrono::Utc::now()),
    };
    db.create_entity(&user_entity).await?;

    let agent_entity = rustliza_core::Entity {
        id: agent_id,
        agent_id,
        names: vec![character.name.clone()],
        metadata: None,
        created_at: Some(chrono::Utc::now()),
    };
    db.create_entity(&agent_entity).await?;

    let room = rustliza_core::Room {
        id: room_id,
        agent_id,
        source: "cli".to_string(),
        channel_type: ChannelType::Dm,
        name: Some("CLI Chat".to_string()),
        channel_id: None,
        world_id: None,
        metadata: None,
        created_at: Some(chrono::Utc::now()),
    };
    db.create_room(&room).await?;
    db.add_participant(user_id, room_id, agent_id).await?;
    db.add_participant(agent_id, room_id, agent_id).await?;

    // REPL
    println!();
    println!("  ╔══════════════════════════════════════════════╗");
    println!("  ║           🦀  R U S T L I Z A  🦀            ║");
    println!("  ║        ElizaOS — rewritten in Rust           ║");
    println!("  ╚══════════════════════════════════════════════╝");
    println!();
    println!("  Agent: {}", character.name);
    if cli.api {
        println!("  API:   http://{}", cli.api_bind);
    }
    if cli.discord_token.is_some() {
        println!("  Discord: connected");
    }
    if cli.telegram_token.is_some() {
        println!("  Telegram: connected");
    }
    if cli.twitter_token.is_some() {
        println!("  Twitter:  connected");
    }
    println!("  Type a message to chat. Ctrl-D or 'exit' to quit.");
    println!();

    let mut rl = DefaultEditor::new()?;
    let prompt = "You > ".to_string();

    loop {
        match rl.readline(&prompt) {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }
                if input == "exit" || input == "quit" || input == "/quit" {
                    println!("Goodbye!");
                    break;
                }

                let _ = rl.add_history_entry(input);

                let message =
                    Memory::new_message(agent_id, user_id, room_id, Content::text(input));

                match runtime.process_message_stream(&message).await {
                    Ok(mut stream) => {
                        let mut got_token = false;
                        print!("\n{} > ", character.name);
                        let _ = std::io::stdout().flush();

                        while let Some(event) = stream.next().await {
                            match event {
                                StreamEvent::Token(token) => {
                                    got_token = true;
                                    print!("{}", token);
                                    let _ = std::io::stdout().flush();
                                }
                                StreamEvent::Done { full_text } => {
                                    if !got_token && !full_text.is_empty() {
                                        print!("{}", full_text);
                                    }
                                    if full_text.is_empty() && !got_token {
                                        print!("({} chose not to respond)", character.name);
                                    }
                                    println!("\n");
                                    break;
                                }
                                StreamEvent::Error(e) => {
                                    eprintln!("\nStream error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "error processing message");
                        eprintln!("Error: {}", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Interrupted");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(e) => {
                error!(error = %e, "readline error");
                break;
            }
        }
    }

    runtime.stop_services().await?;
    Ok(())
}
