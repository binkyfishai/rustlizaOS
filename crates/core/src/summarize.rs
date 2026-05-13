use crate::traits::ModelProvider;
use crate::types::*;

const SUMMARIZE_TEMPLATE: &str = r#"Summarize the following conversation in 2-3 concise sentences. Focus on the key topics discussed, decisions made, and any important context. Do not include greetings or filler.

Conversation:
{{conversation}}"#;

pub fn should_summarize(messages: &[Memory], threshold: usize) -> bool {
    messages.len() > threshold
}

pub fn split_for_summary(
    messages: &[Memory],
    keep_recent: usize,
) -> (&[Memory], &[Memory]) {
    if messages.len() <= keep_recent {
        return (&[], messages);
    }
    let split_point = messages.len() - keep_recent;
    (&messages[..split_point], &messages[split_point..])
}

pub async fn summarize_messages(
    messages: &[Memory],
    agent_name: &str,
    model_provider: &dyn ModelProvider,
) -> crate::error::Result<String> {
    if messages.is_empty() {
        return Ok(String::new());
    }

    let conversation: String = messages
        .iter()
        .map(|m| {
            let sender = if m.entity_id == m.agent_id {
                agent_name.to_string()
            } else {
                format!("User({})", &m.entity_id.to_string()[..8])
            };
            let text = m.content.text.as_deref().unwrap_or("[no text]");
            format!("{}: {}", sender, text)
        })
        .collect::<Vec<_>>()
        .join("\n");

    let prompt = SUMMARIZE_TEMPLATE.replace("{{conversation}}", &conversation);

    model_provider
        .generate_text(&GenerateTextParams {
            model_type: ModelType::TextSmall,
            system_prompt: "You are a concise conversation summarizer.".into(),
            prompt,
            max_tokens: Some(256),
            temperature: Some(0.3),
            stop_sequences: vec![],
        })
        .await
}

pub fn format_with_summary(
    summary: &str,
    recent_messages: &[Memory],
    agent_name: &str,
) -> String {
    let mut lines = Vec::new();

    if !summary.is_empty() {
        lines.push(format!("[Earlier conversation summary: {}]", summary));
        lines.push(String::new());
    }

    for mem in recent_messages {
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
