use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::error::{Result, RustlizaError};
use crate::types::Content;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageExample {
    pub user: String,
    pub content: Content,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StyleConfig {
    #[serde(default)]
    pub all: Vec<String>,
    #[serde(default)]
    pub chat: Vec<String>,
    #[serde(default)]
    pub post: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VoiceSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CharacterSettings {
    #[serde(default)]
    pub secrets: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<VoiceSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_knowledge: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_autonomy: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Character {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,

    #[serde(default)]
    pub bio: Vec<String>,

    #[serde(default)]
    pub lore: Vec<String>,

    #[serde(default)]
    pub knowledge: Vec<String>,

    #[serde(default, rename = "messageExamples")]
    pub message_examples: Vec<Vec<MessageExample>>,

    #[serde(default, rename = "postExamples")]
    pub post_examples: Vec<String>,

    #[serde(default)]
    pub topics: Vec<String>,

    #[serde(default)]
    pub adjectives: Vec<String>,

    #[serde(default)]
    pub style: StyleConfig,

    #[serde(default)]
    pub plugins: Vec<String>,

    #[serde(default)]
    pub settings: CharacterSettings,
}

impl Character {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let data = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            RustlizaError::CharacterLoad(format!(
                "failed to read {}: {}",
                path.as_ref().display(),
                e
            ))
        })?;
        let character: Character = serde_json::from_str(&data).map_err(|e| {
            RustlizaError::CharacterLoad(format!(
                "failed to parse {}: {}",
                path.as_ref().display(),
                e
            ))
        })?;
        Ok(character)
    }

    pub fn bio_text(&self) -> String {
        self.bio.join(" ")
    }

    pub fn lore_text(&self) -> String {
        self.lore.join("\n")
    }

    pub fn style_all_text(&self) -> String {
        self.style.all.join("\n")
    }

    pub fn style_chat_text(&self) -> String {
        self.style.chat.join("\n")
    }

    pub fn adjectives_text(&self) -> String {
        self.adjectives.join(", ")
    }

    pub fn topics_text(&self) -> String {
        self.topics.join(", ")
    }

    pub fn format_message_examples(&self) -> String {
        let mut out = String::new();
        for (i, conversation) in self.message_examples.iter().enumerate() {
            if i > 0 {
                out.push_str("\n---\n");
            }
            for msg in conversation {
                let text = msg.content.text.as_deref().unwrap_or("");
                out.push_str(&format!("{}: {}\n", msg.user, text));
            }
        }
        out
    }
}

impl Default for Character {
    fn default() -> Self {
        Self {
            name: "Eliza".to_string(),
            username: None,
            system: None,
            bio: vec!["A helpful AI assistant.".to_string()],
            lore: vec![],
            knowledge: vec![],
            message_examples: vec![],
            post_examples: vec![],
            topics: vec![],
            adjectives: vec!["helpful".to_string(), "friendly".to_string()],
            style: StyleConfig::default(),
            plugins: vec![],
            settings: CharacterSettings::default(),
        }
    }
}
