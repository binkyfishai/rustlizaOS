use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::Row;
use tracing::info;
use uuid::Uuid;

use rustliza_core::error::{Result, RustlizaError};
use rustliza_core::traits::DatabaseAdapter;
use rustliza_core::types::*;

pub struct SqliteAdapter {
    pool: SqlitePool,
}

impl SqliteAdapter {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_str().unwrap_or("rustliza.db");
        let options = SqliteConnectOptions::from_str(&format!("sqlite:{}", db_path))
            .map_err(|e| RustlizaError::Database(e.into()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(std::time::Duration::from_secs(5))
            .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
            .pragma("cache_size", "-8000")
            .pragma("temp_store", "memory");

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .map_err(|e| RustlizaError::Database(e.into()))?;

        Ok(Self { pool })
    }

    pub async fn new_in_memory() -> Result<Self> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| RustlizaError::Database(e.into()))?;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| RustlizaError::Database(e.into()))?;

        Ok(Self { pool })
    }
}

fn db_err(e: sqlx::Error) -> RustlizaError {
    RustlizaError::Database(e.into())
}

fn uuid_to_str(u: &Uuid) -> String {
    u.to_string()
}

fn str_to_uuid(s: &str) -> std::result::Result<Uuid, RustlizaError> {
    Uuid::parse_str(s)
        .map_err(|e| RustlizaError::Database(anyhow::anyhow!("invalid uuid '{}': {}", s, e)))
}

fn parse_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS entities (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    names TEXT NOT NULL DEFAULT '[]',
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_entities_agent_id ON entities(agent_id);

CREATE TABLE IF NOT EXISTS rooms (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT '',
    channel_type TEXT NOT NULL DEFAULT 'DM',
    name TEXT,
    channel_id TEXT,
    world_id TEXT,
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_rooms_agent_id ON rooms(agent_id);
CREATE INDEX IF NOT EXISTS idx_rooms_world_id ON rooms(world_id);

CREATE TABLE IF NOT EXISTS worlds (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    metadata TEXT,
    server_id TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_worlds_agent_id ON worlds(agent_id);

CREATE TABLE IF NOT EXISTS memories (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL DEFAULT 'message',
    content TEXT NOT NULL DEFAULT '{}',
    entity_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    world_id TEXT,
    "unique" INTEGER NOT NULL DEFAULT 0,
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_memories_agent_id ON memories(agent_id);
CREATE INDEX IF NOT EXISTS idx_memories_room_id ON memories(room_id);
CREATE INDEX IF NOT EXISTS idx_memories_entity_id ON memories(entity_id);
CREATE INDEX IF NOT EXISTS idx_memories_type ON memories(type);
CREATE INDEX IF NOT EXISTS idx_memories_created_at ON memories(created_at);
CREATE INDEX IF NOT EXISTS idx_memories_room_type ON memories(room_id, type, created_at);

CREATE TABLE IF NOT EXISTS embeddings (
    id TEXT PRIMARY KEY,
    memory_id TEXT NOT NULL UNIQUE,
    embedding BLOB,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (memory_id) REFERENCES memories(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS participants (
    id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    room_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    room_state TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE(entity_id, room_id)
);
CREATE INDEX IF NOT EXISTS idx_participants_entity_id ON participants(entity_id);
CREATE INDEX IF NOT EXISTS idx_participants_room_id ON participants(room_id);

CREATE TABLE IF NOT EXISTS relationships (
    id TEXT PRIMARY KEY,
    source_entity_id TEXT NOT NULL,
    target_entity_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    metadata TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE(source_entity_id, target_entity_id, agent_id)
);
CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_entity_id);
CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_entity_id);

CREATE TABLE IF NOT EXISTS components (
    id TEXT PRIMARY KEY,
    entity_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    room_id TEXT,
    world_id TEXT,
    source_entity_id TEXT,
    component_type TEXT NOT NULL,
    data TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE(entity_id, component_type, world_id, source_entity_id)
);
CREATE INDEX IF NOT EXISTS idx_components_entity ON components(entity_id);
CREATE INDEX IF NOT EXISTS idx_components_type ON components(component_type);

CREATE TABLE IF NOT EXISTS cache (
    key TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    value TEXT NOT NULL DEFAULT '{}',
    expires_at TEXT,
    PRIMARY KEY (key, agent_id)
);
"#;

// ---------------------------------------------------------------------------
// Row parsing helpers
// ---------------------------------------------------------------------------

fn parse_entity_row(row: &SqliteRow) -> std::result::Result<Entity, sqlx::Error> {
    let id_str: String = row.try_get("id")?;
    let agent_str: String = row.try_get("agent_id")?;
    let names_json: String = row.try_get("names")?;
    let metadata: Option<String> = row.try_get("metadata")?;
    let created_at: Option<String> = row.try_get("created_at")?;

    Ok(Entity {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        agent_id: Uuid::parse_str(&agent_str).unwrap_or_default(),
        names: serde_json::from_str(&names_json).unwrap_or_default(),
        metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
        created_at: created_at.and_then(|s| parse_datetime(&s)),
    })
}

fn parse_memory_row(row: &SqliteRow) -> std::result::Result<Memory, sqlx::Error> {
    let id_str: String = row.try_get("id")?;
    let type_str: String = row.try_get("type")?;
    let content_json: String = row.try_get("content")?;
    let entity_str: String = row.try_get("entity_id")?;
    let agent_str: String = row.try_get("agent_id")?;
    let room_str: String = row.try_get("room_id")?;
    let world_str: Option<String> = row.try_get("world_id")?;
    let unique_int: i32 = row.try_get("unique")?;
    let metadata: Option<String> = row.try_get("metadata")?;
    let created_at: Option<String> = row.try_get("created_at")?;

    let memory_type = match type_str.as_str() {
        "document" => MemoryType::Document,
        "fragment" => MemoryType::Fragment,
        "description" => MemoryType::Description,
        "custom" => MemoryType::Custom,
        _ => MemoryType::Message,
    };

    Ok(Memory {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        content: serde_json::from_str(&content_json).unwrap_or_default(),
        entity_id: Uuid::parse_str(&entity_str).unwrap_or_default(),
        agent_id: Uuid::parse_str(&agent_str).unwrap_or_default(),
        room_id: Uuid::parse_str(&room_str).unwrap_or_default(),
        world_id: world_str.and_then(|s| Uuid::parse_str(&s).ok()),
        unique: unique_int != 0,
        created_at: created_at.and_then(|s| parse_datetime(&s)),
        embedding: None,
        metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
        memory_type,
    })
}

fn parse_room_row(row: &SqliteRow) -> std::result::Result<Room, sqlx::Error> {
    let id_str: String = row.try_get("id")?;
    let agent_str: String = row.try_get("agent_id")?;
    let source: String = row.try_get("source")?;
    let channel_type_str: String = row.try_get("channel_type")?;
    let name: Option<String> = row.try_get("name")?;
    let channel_id: Option<String> = row.try_get("channel_id")?;
    let world_str: Option<String> = row.try_get("world_id")?;
    let metadata: Option<String> = row.try_get("metadata")?;
    let created_at: Option<String> = row.try_get("created_at")?;

    let channel_type = serde_json::from_str::<ChannelType>(&format!("\"{}\"", channel_type_str))
        .unwrap_or_default();

    Ok(Room {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        agent_id: Uuid::parse_str(&agent_str).unwrap_or_default(),
        source,
        channel_type,
        name,
        channel_id,
        world_id: world_str.and_then(|s| Uuid::parse_str(&s).ok()),
        metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
        created_at: created_at.and_then(|s| parse_datetime(&s)),
    })
}

fn parse_relationship_row(row: &SqliteRow) -> std::result::Result<Relationship, sqlx::Error> {
    let id_str: String = row.try_get("id")?;
    let source_str: String = row.try_get("source_entity_id")?;
    let target_str: String = row.try_get("target_entity_id")?;
    let agent_str: String = row.try_get("agent_id")?;
    let tags_json: String = row.try_get("tags")?;
    let metadata: Option<String> = row.try_get("metadata")?;
    let created_at: Option<String> = row.try_get("created_at")?;

    Ok(Relationship {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        source_entity_id: Uuid::parse_str(&source_str).unwrap_or_default(),
        target_entity_id: Uuid::parse_str(&target_str).unwrap_or_default(),
        agent_id: Uuid::parse_str(&agent_str).unwrap_or_default(),
        tags: serde_json::from_str(&tags_json).unwrap_or_default(),
        metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
        created_at: created_at.and_then(|s| parse_datetime(&s)),
    })
}

fn parse_component_row(row: &SqliteRow) -> std::result::Result<Component, sqlx::Error> {
    let id_str: String = row.try_get("id")?;
    let entity_str: String = row.try_get("entity_id")?;
    let agent_str: String = row.try_get("agent_id")?;
    let room_str: Option<String> = row.try_get("room_id")?;
    let world_str: Option<String> = row.try_get("world_id")?;
    let source_str: Option<String> = row.try_get("source_entity_id")?;
    let comp_type: String = row.try_get("component_type")?;
    let data: Option<String> = row.try_get("data")?;
    let created_at: Option<String> = row.try_get("created_at")?;

    Ok(Component {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        entity_id: Uuid::parse_str(&entity_str).unwrap_or_default(),
        agent_id: Uuid::parse_str(&agent_str).unwrap_or_default(),
        room_id: room_str.and_then(|s| Uuid::parse_str(&s).ok()),
        world_id: world_str.and_then(|s| Uuid::parse_str(&s).ok()),
        source_entity_id: source_str.and_then(|s| Uuid::parse_str(&s).ok()),
        component_type: comp_type,
        data: data.and_then(|s| serde_json::from_str(&s).ok()),
        created_at: created_at.and_then(|s| parse_datetime(&s)),
    })
}

fn parse_world_row(row: &SqliteRow) -> std::result::Result<World, sqlx::Error> {
    let id_str: String = row.try_get("id")?;
    let agent_str: String = row.try_get("agent_id")?;
    let name: String = row.try_get("name")?;
    let metadata: Option<String> = row.try_get("metadata")?;
    let server_id: Option<String> = row.try_get("server_id")?;
    let created_at: Option<String> = row.try_get("created_at")?;

    Ok(World {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        agent_id: Uuid::parse_str(&agent_str).unwrap_or_default(),
        name,
        metadata: metadata.and_then(|s| serde_json::from_str(&s).ok()),
        server_id,
        created_at: created_at.and_then(|s| parse_datetime(&s)),
    })
}

// ---------------------------------------------------------------------------
// DatabaseAdapter implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl DatabaseAdapter for SqliteAdapter {
    async fn init(&self) -> Result<()> {
        sqlx::raw_sql(SCHEMA)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        info!("database schema initialized");
        Ok(())
    }

    // -- Entity ---------------------------------------------------------------

    async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>> {
        let row = sqlx::query("SELECT * FROM entities WHERE id = ?")
            .bind(uuid_to_str(&id))
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;

        match row {
            Some(r) => Ok(Some(parse_entity_row(&r).map_err(db_err)?)),
            None => Ok(None),
        }
    }

    async fn create_entity(&self, entity: &Entity) -> Result<()> {
        let names_json = serde_json::to_string(&entity.names)?;
        let metadata_json = entity
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;

        sqlx::query(
            "INSERT OR IGNORE INTO entities (id, agent_id, names, metadata) VALUES (?, ?, ?, ?)",
        )
        .bind(uuid_to_str(&entity.id))
        .bind(uuid_to_str(&entity.agent_id))
        .bind(&names_json)
        .bind(&metadata_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(())
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        let names_json = serde_json::to_string(&entity.names)?;
        let metadata_json = entity
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;

        sqlx::query("UPDATE entities SET names = ?, metadata = ? WHERE id = ?")
            .bind(&names_json)
            .bind(&metadata_json)
            .bind(uuid_to_str(&entity.id))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;

        Ok(())
    }

    // -- Memory ---------------------------------------------------------------

    async fn create_memory(&self, memory: &Memory) -> Result<()> {
        let content_json = serde_json::to_string(&memory.content)?;
        let metadata_json = memory
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;
        let type_str = serde_json::to_string(&memory.memory_type)?
            .trim_matches('"')
            .to_string();
        let world_str = memory.world_id.map(|w| uuid_to_str(&w));

        sqlx::query(
            r#"INSERT OR REPLACE INTO memories
               (id, type, content, entity_id, agent_id, room_id, world_id, "unique", metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(uuid_to_str(&memory.id))
        .bind(&type_str)
        .bind(&content_json)
        .bind(uuid_to_str(&memory.entity_id))
        .bind(uuid_to_str(&memory.agent_id))
        .bind(uuid_to_str(&memory.room_id))
        .bind(&world_str)
        .bind(memory.unique as i32)
        .bind(&metadata_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        // Store embedding if present
        if let Some(embedding) = &memory.embedding {
            let embedding_bytes = embedding_to_bytes(embedding);
            sqlx::query(
                "INSERT OR REPLACE INTO embeddings (id, memory_id, embedding) VALUES (?, ?, ?)",
            )
            .bind(uuid_to_str(&Uuid::new_v4()))
            .bind(uuid_to_str(&memory.id))
            .bind(&embedding_bytes)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        }

        Ok(())
    }

    async fn get_memories(&self, params: &GetMemoriesParams) -> Result<Vec<Memory>> {
        let mut sql = String::from("SELECT * FROM memories WHERE 1=1");
        let mut bindings: Vec<String> = Vec::new();

        if let Some(room_id) = &params.room_id {
            sql.push_str(" AND room_id = ?");
            bindings.push(uuid_to_str(room_id));
        }
        if let Some(agent_id) = &params.agent_id {
            sql.push_str(" AND agent_id = ?");
            bindings.push(uuid_to_str(agent_id));
        }
        if let Some(entity_id) = &params.entity_id {
            sql.push_str(" AND entity_id = ?");
            bindings.push(uuid_to_str(entity_id));
        }
        if let Some(memory_type) = &params.memory_type {
            let type_str = serde_json::to_string(memory_type)?
                .trim_matches('"')
                .to_string();
            sql.push_str(" AND type = ?");
            bindings.push(type_str);
        }
        if let Some(unique) = params.unique {
            sql.push_str(" AND \"unique\" = ?");
            bindings.push((unique as i32).to_string());
        }

        sql.push_str(" ORDER BY created_at DESC");

        if let Some(count) = params.count {
            sql.push_str(&format!(" LIMIT {}", count));
        }

        let mut query = sqlx::query(&sql);
        for b in &bindings {
            query = query.bind(b);
        }

        let rows = query.fetch_all(&self.pool).await.map_err(db_err)?;

        let mut memories = Vec::new();
        for row in &rows {
            memories.push(parse_memory_row(row).map_err(db_err)?);
        }

        Ok(memories)
    }

    async fn get_memory_by_id(&self, id: MemoryId) -> Result<Option<Memory>> {
        let row = sqlx::query("SELECT * FROM memories WHERE id = ?")
            .bind(uuid_to_str(&id))
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;

        match row {
            Some(r) => Ok(Some(parse_memory_row(&r).map_err(db_err)?)),
            None => Ok(None),
        }
    }

    async fn search_memories(&self, params: &SearchMemoriesParams) -> Result<Vec<Memory>> {
        // Fetch all embeddings and compute similarity in Rust
        // (SQLite doesn't have native vector operations)
        let mut sql = String::from(
            "SELECT m.*, e.embedding FROM memories m
             INNER JOIN embeddings e ON e.memory_id = m.id
             WHERE m.agent_id = ?",
        );
        let mut bindings = vec![uuid_to_str(&params.agent_id)];

        if let Some(room_id) = &params.room_id {
            sql.push_str(" AND m.room_id = ?");
            bindings.push(uuid_to_str(room_id));
        }
        if let Some(memory_type) = &params.memory_type {
            let type_str = serde_json::to_string(memory_type)?
                .trim_matches('"')
                .to_string();
            sql.push_str(" AND m.type = ?");
            bindings.push(type_str);
        }
        if let Some(unique) = params.unique {
            sql.push_str(" AND m.\"unique\" = ?");
            bindings.push((unique as i32).to_string());
        }

        let mut query = sqlx::query(&sql);
        for b in &bindings {
            query = query.bind(b);
        }

        let rows = query.fetch_all(&self.pool).await.map_err(db_err)?;

        let mut scored: Vec<(Memory, f32)> = Vec::new();
        for row in &rows {
            let mut memory = parse_memory_row(row).map_err(db_err)?;
            let embedding_bytes: Option<Vec<u8>> = row.try_get("embedding").ok();
            if let Some(bytes) = embedding_bytes {
                let stored_embedding = bytes_to_embedding(&bytes);
                let similarity =
                    rustliza_core::search::cosine_similarity(&params.embedding, &stored_embedding);
                if similarity >= params.match_threshold {
                    memory.embedding = Some(stored_embedding);
                    scored.push((memory, similarity));
                }
            }
        }

        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(params.match_count);

        Ok(scored.into_iter().map(|(m, _)| m).collect())
    }

    async fn delete_memory(&self, id: MemoryId) -> Result<()> {
        sqlx::query("DELETE FROM memories WHERE id = ?")
            .bind(uuid_to_str(&id))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn count_memories(
        &self,
        room_id: RoomId,
        unique: bool,
        memory_type: MemoryType,
    ) -> Result<usize> {
        let type_str = serde_json::to_string(&memory_type)?
            .trim_matches('"')
            .to_string();

        let row = sqlx::query(
            r#"SELECT COUNT(*) as cnt FROM memories
               WHERE room_id = ? AND "unique" = ? AND type = ?"#,
        )
        .bind(uuid_to_str(&room_id))
        .bind(unique as i32)
        .bind(&type_str)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;

        let count: i64 = row.try_get("cnt").map_err(db_err)?;
        Ok(count as usize)
    }

    // -- Room -----------------------------------------------------------------

    async fn get_room(&self, id: RoomId) -> Result<Option<Room>> {
        let row = sqlx::query("SELECT * FROM rooms WHERE id = ?")
            .bind(uuid_to_str(&id))
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;

        match row {
            Some(r) => Ok(Some(parse_room_row(&r).map_err(db_err)?)),
            None => Ok(None),
        }
    }

    async fn create_room(&self, room: &Room) -> Result<RoomId> {
        let metadata_json = room
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;
        let world_str = room.world_id.map(|w| uuid_to_str(&w));
        let channel_type_str =
            serde_json::to_string(&room.channel_type)?.trim_matches('"').to_string();

        sqlx::query(
            "INSERT OR IGNORE INTO rooms (id, agent_id, source, channel_type, name, channel_id, world_id, metadata)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(uuid_to_str(&room.id))
        .bind(uuid_to_str(&room.agent_id))
        .bind(&room.source)
        .bind(&channel_type_str)
        .bind(&room.name)
        .bind(&room.channel_id)
        .bind(&world_str)
        .bind(&metadata_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(room.id)
    }

    async fn get_rooms_for_participant(&self, entity_id: EntityId) -> Result<Vec<RoomId>> {
        let rows = sqlx::query("SELECT room_id FROM participants WHERE entity_id = ?")
            .bind(uuid_to_str(&entity_id))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;

        let mut ids = Vec::new();
        for row in &rows {
            let s: String = row.try_get("room_id").map_err(db_err)?;
            ids.push(str_to_uuid(&s)?);
        }
        Ok(ids)
    }

    async fn get_participants_for_room(&self, room_id: RoomId) -> Result<Vec<EntityId>> {
        let rows = sqlx::query("SELECT entity_id FROM participants WHERE room_id = ?")
            .bind(uuid_to_str(&room_id))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;

        let mut ids = Vec::new();
        for row in &rows {
            let s: String = row.try_get("entity_id").map_err(db_err)?;
            ids.push(str_to_uuid(&s)?);
        }
        Ok(ids)
    }

    async fn add_participant(
        &self,
        entity_id: EntityId,
        room_id: RoomId,
        agent_id: AgentId,
    ) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO participants (id, entity_id, room_id, agent_id) VALUES (?, ?, ?, ?)",
        )
        .bind(uuid_to_str(&Uuid::new_v4()))
        .bind(uuid_to_str(&entity_id))
        .bind(uuid_to_str(&room_id))
        .bind(uuid_to_str(&agent_id))
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn remove_participant(&self, entity_id: EntityId, room_id: RoomId) -> Result<()> {
        sqlx::query("DELETE FROM participants WHERE entity_id = ? AND room_id = ?")
            .bind(uuid_to_str(&entity_id))
            .bind(uuid_to_str(&room_id))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    // -- Relationship ---------------------------------------------------------

    async fn get_relationship(
        &self,
        source: EntityId,
        target: EntityId,
        agent_id: AgentId,
    ) -> Result<Option<Relationship>> {
        let row = sqlx::query(
            "SELECT * FROM relationships WHERE source_entity_id = ? AND target_entity_id = ? AND agent_id = ?",
        )
        .bind(uuid_to_str(&source))
        .bind(uuid_to_str(&target))
        .bind(uuid_to_str(&agent_id))
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        match row {
            Some(r) => Ok(Some(parse_relationship_row(&r).map_err(db_err)?)),
            None => Ok(None),
        }
    }

    async fn get_relationships(&self, entity_id: EntityId) -> Result<Vec<Relationship>> {
        let rows = sqlx::query(
            "SELECT * FROM relationships WHERE source_entity_id = ? OR target_entity_id = ?",
        )
        .bind(uuid_to_str(&entity_id))
        .bind(uuid_to_str(&entity_id))
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;

        let mut rels = Vec::new();
        for row in &rows {
            rels.push(parse_relationship_row(row).map_err(db_err)?);
        }
        Ok(rels)
    }

    async fn create_relationship(&self, rel: &Relationship) -> Result<()> {
        let tags_json = serde_json::to_string(&rel.tags)?;
        let metadata_json = rel
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;

        sqlx::query(
            "INSERT OR IGNORE INTO relationships (id, source_entity_id, target_entity_id, agent_id, tags, metadata)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(uuid_to_str(&rel.id))
        .bind(uuid_to_str(&rel.source_entity_id))
        .bind(uuid_to_str(&rel.target_entity_id))
        .bind(uuid_to_str(&rel.agent_id))
        .bind(&tags_json)
        .bind(&metadata_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(())
    }

    // -- Component ------------------------------------------------------------

    async fn get_components(
        &self,
        entity_id: EntityId,
        component_type: Option<&str>,
    ) -> Result<Vec<Component>> {
        let (sql, bindings) = if let Some(ct) = component_type {
            (
                "SELECT * FROM components WHERE entity_id = ? AND component_type = ?".to_string(),
                vec![uuid_to_str(&entity_id), ct.to_string()],
            )
        } else {
            (
                "SELECT * FROM components WHERE entity_id = ?".to_string(),
                vec![uuid_to_str(&entity_id)],
            )
        };

        let mut query = sqlx::query(&sql);
        for b in &bindings {
            query = query.bind(b);
        }

        let rows = query.fetch_all(&self.pool).await.map_err(db_err)?;

        let mut components = Vec::new();
        for row in &rows {
            components.push(parse_component_row(row).map_err(db_err)?);
        }
        Ok(components)
    }

    async fn create_component(&self, component: &Component) -> Result<()> {
        let data_json = component
            .data
            .as_ref()
            .map(|d| serde_json::to_string(d))
            .transpose()?;
        let room_str = component.room_id.map(|r| uuid_to_str(&r));
        let world_str = component.world_id.map(|w| uuid_to_str(&w));
        let source_str = component.source_entity_id.map(|s| uuid_to_str(&s));

        sqlx::query(
            "INSERT OR IGNORE INTO components (id, entity_id, agent_id, room_id, world_id, source_entity_id, component_type, data)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(uuid_to_str(&component.id))
        .bind(uuid_to_str(&component.entity_id))
        .bind(uuid_to_str(&component.agent_id))
        .bind(&room_str)
        .bind(&world_str)
        .bind(&source_str)
        .bind(&component.component_type)
        .bind(&data_json)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(())
    }

    async fn update_component(&self, component: &Component) -> Result<()> {
        let data_json = component
            .data
            .as_ref()
            .map(|d| serde_json::to_string(d))
            .transpose()?;

        sqlx::query("UPDATE components SET data = ? WHERE id = ?")
            .bind(&data_json)
            .bind(uuid_to_str(&component.id))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;

        Ok(())
    }

    async fn delete_component(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM components WHERE id = ?")
            .bind(uuid_to_str(&id))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    // -- World ----------------------------------------------------------------

    async fn get_world(&self, id: WorldId) -> Result<Option<World>> {
        let row = sqlx::query("SELECT * FROM worlds WHERE id = ?")
            .bind(uuid_to_str(&id))
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;

        match row {
            Some(r) => Ok(Some(parse_world_row(&r).map_err(db_err)?)),
            None => Ok(None),
        }
    }

    async fn create_world(&self, world: &World) -> Result<WorldId> {
        let metadata_json = world
            .metadata
            .as_ref()
            .map(|m| serde_json::to_string(m))
            .transpose()?;

        sqlx::query(
            "INSERT OR IGNORE INTO worlds (id, agent_id, name, metadata, server_id) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(uuid_to_str(&world.id))
        .bind(uuid_to_str(&world.agent_id))
        .bind(&world.name)
        .bind(&metadata_json)
        .bind(&world.server_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(world.id)
    }

    // -- Cache ----------------------------------------------------------------

    async fn get_cache(&self, key: &str, agent_id: AgentId) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query(
            "SELECT value, expires_at FROM cache WHERE key = ? AND agent_id = ?",
        )
        .bind(key)
        .bind(uuid_to_str(&agent_id))
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;

        match row {
            Some(r) => {
                // Check expiry
                let expires_at: Option<String> = r.try_get("expires_at").map_err(db_err)?;
                if let Some(exp) = expires_at {
                    if let Some(dt) = parse_datetime(&exp) {
                        if dt < Utc::now() {
                            // Expired — clean up
                            let _ = self.delete_cache(key, agent_id).await;
                            return Ok(None);
                        }
                    }
                }
                let value_str: String = r.try_get("value").map_err(db_err)?;
                let value: serde_json::Value = serde_json::from_str(&value_str)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn set_cache(
        &self,
        key: &str,
        agent_id: AgentId,
        value: &serde_json::Value,
        expires_at: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let value_str = serde_json::to_string(value)?;
        let expires_str = expires_at.map(|dt| dt.to_rfc3339());

        sqlx::query(
            "INSERT OR REPLACE INTO cache (key, agent_id, value, expires_at) VALUES (?, ?, ?, ?)",
        )
        .bind(key)
        .bind(uuid_to_str(&agent_id))
        .bind(&value_str)
        .bind(&expires_str)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;

        Ok(())
    }

    async fn delete_cache(&self, key: &str, agent_id: AgentId) -> Result<()> {
        sqlx::query("DELETE FROM cache WHERE key = ? AND agent_id = ?")
            .bind(key)
            .bind(uuid_to_str(&agent_id))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Embedding serialisation helpers
// ---------------------------------------------------------------------------

fn embedding_to_bytes(embedding: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(embedding.len() * 4);
    for &val in embedding {
        bytes.extend_from_slice(&val.to_le_bytes());
    }
    bytes
}

fn bytes_to_embedding(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(4)
        .map(|chunk| {
            let arr: [u8; 4] = chunk.try_into().unwrap();
            f32::from_le_bytes(arr)
        })
        .collect()
}
