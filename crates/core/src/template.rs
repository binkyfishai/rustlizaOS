use std::collections::HashMap;

use regex::Regex;

/// Handlebars-style template engine supporting:
///   - `{{variable}}` — simple substitution
///   - `{{#if variable}}...{{/if}}` — conditional blocks
///   - `{{#each variable}}...{{/each}}` — iteration (value split by `\n`)
///   - `{{#unless variable}}...{{/unless}}` — negated conditional
pub fn render(template: &str, vars: &HashMap<String, String>) -> String {
    let mut result = template.to_string();

    // Process {{#each variable}}...{{/each}} blocks
    let each_re = Regex::new(r"\{\{#each\s+(\w+)\}\}([\s\S]*?)\{\{/each\}\}").unwrap();
    result = each_re
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let body = &caps[2];
            match vars.get(var_name) {
                Some(val) if !val.is_empty() => val
                    .lines()
                    .map(|line| {
                        body.replace("{{this}}", line)
                    })
                    .collect::<Vec<_>>()
                    .join(""),
                _ => String::new(),
            }
        })
        .to_string();

    // Process {{#unless variable}}...{{/unless}} blocks
    let unless_re = Regex::new(r"\{\{#unless\s+(\w+)\}\}([\s\S]*?)\{\{/unless\}\}").unwrap();
    result = unless_re
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let body = &caps[2];
            match vars.get(var_name) {
                Some(val) if !val.is_empty() => String::new(),
                _ => body.to_string(),
            }
        })
        .to_string();

    // Process {{#if variable}}...{{else}}...{{/if}} blocks
    let if_else_re =
        Regex::new(r"\{\{#if\s+(\w+)\}\}([\s\S]*?)\{\{else\}\}([\s\S]*?)\{\{/if\}\}").unwrap();
    result = if_else_re
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let if_body = &caps[2];
            let else_body = &caps[3];
            match vars.get(var_name) {
                Some(val) if !val.is_empty() => if_body.to_string(),
                _ => else_body.to_string(),
            }
        })
        .to_string();

    // Process {{#if variable}}...{{/if}} blocks (no else)
    let if_re = Regex::new(r"\{\{#if\s+(\w+)\}\}([\s\S]*?)\{\{/if\}\}").unwrap();
    result = if_re
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            let body = &caps[2];
            match vars.get(var_name) {
                Some(val) if !val.is_empty() => body.to_string(),
                _ => String::new(),
            }
        })
        .to_string();

    // Process simple {{variable}} substitutions
    let var_re = Regex::new(r"\{\{(\w+)\}\}").unwrap();
    result = var_re
        .replace_all(&result, |caps: &regex::Captures| {
            let var_name = &caps[1];
            vars.get(var_name).cloned().unwrap_or_default()
        })
        .to_string();

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_substitution() {
        let mut vars = HashMap::new();
        vars.insert("name".into(), "Eliza".into());
        vars.insert("bio".into(), "A helpful AI.".into());
        assert_eq!(
            render("Hello, I am {{name}}. {{bio}}", &vars),
            "Hello, I am Eliza. A helpful AI."
        );
    }

    #[test]
    fn if_block() {
        let mut vars = HashMap::new();
        vars.insert("lore".into(), "Some backstory".into());
        let template = "{{#if lore}}Lore: {{lore}}{{/if}}";
        assert_eq!(render(template, &vars), "Lore: Some backstory");

        let empty: HashMap<String, String> = HashMap::new();
        assert_eq!(render(template, &empty), "");
    }

    #[test]
    fn if_else_block() {
        let mut vars = HashMap::new();
        vars.insert("name".into(), "Eliza".into());
        let template = "{{#if name}}Hi {{name}}{{else}}Hi stranger{{/if}}";
        assert_eq!(render(template, &vars), "Hi Eliza");

        let empty: HashMap<String, String> = HashMap::new();
        assert_eq!(render(template, &empty), "Hi stranger");
    }

    #[test]
    fn each_block() {
        let mut vars = HashMap::new();
        vars.insert("items".into(), "apple\nbanana\ncherry".into());
        let template = "{{#each items}}- {{this}}\n{{/each}}";
        assert_eq!(render(template, &vars), "- apple\n- banana\n- cherry\n");
    }

    #[test]
    fn unless_block() {
        let empty: HashMap<String, String> = HashMap::new();
        let template = "{{#unless name}}No name set{{/unless}}";
        assert_eq!(render(template, &empty), "No name set");

        let mut vars = HashMap::new();
        vars.insert("name".into(), "Eliza".into());
        assert_eq!(render(template, &vars), "");
    }
}
