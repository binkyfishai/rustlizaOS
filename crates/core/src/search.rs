use std::collections::HashMap;

use unicode_normalization::UnicodeNormalization;

// ---------------------------------------------------------------------------
// Tokeniser with stop-word removal and Porter2-style stemming
// ---------------------------------------------------------------------------

const STOP_WORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is",
    "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there",
    "these", "they", "this", "to", "was", "will", "with",
];

fn is_stop_word(word: &str) -> bool {
    STOP_WORDS.binary_search(&word).is_ok()
}

fn normalize(text: &str) -> String {
    text.nfkd()
        .filter(|c| !c.is_ascii_punctuation() || *c == '\'')
        .collect::<String>()
        .to_lowercase()
}

fn stem(word: &str) -> String {
    let mut w = word.to_string();
    if w.len() <= 3 {
        return w;
    }
    for suffix in &["ement", "ment", "ness", "tion", "sion", "ling", "ing", "ies", "ous", "ive", "able", "ful", "less", "ated", "ize", "ise", "ate", "ly", "ed", "er", "es", "al", "en", "s"] {
        if w.len() > suffix.len() + 2 && w.ends_with(suffix) {
            w.truncate(w.len() - suffix.len());
            break;
        }
    }
    w
}

pub fn tokenize(text: &str) -> Vec<String> {
    let normalized = normalize(text);
    normalized
        .split_whitespace()
        .filter(|w| w.len() >= 2)
        .filter(|w| !is_stop_word(w))
        .map(|w| stem(w))
        .collect()
}

// ---------------------------------------------------------------------------
// BM25 search engine
// ---------------------------------------------------------------------------

pub struct BM25 {
    k1: f64,
    b: f64,
    documents: Vec<Vec<String>>,
    doc_lengths: Vec<usize>,
    avg_doc_length: f64,
    df: HashMap<String, usize>,
    n: usize,
}

impl BM25 {
    pub fn new() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            documents: vec![],
            doc_lengths: vec![],
            avg_doc_length: 0.0,
            df: HashMap::new(),
            n: 0,
        }
    }

    pub fn add_document(&mut self, text: &str) -> usize {
        let tokens = tokenize(text);
        let doc_idx = self.documents.len();

        let mut seen = std::collections::HashSet::new();
        for token in &tokens {
            if seen.insert(token.clone()) {
                *self.df.entry(token.clone()).or_insert(0) += 1;
            }
        }

        self.doc_lengths.push(tokens.len());
        self.documents.push(tokens);
        self.n += 1;
        self.avg_doc_length =
            self.doc_lengths.iter().sum::<usize>() as f64 / self.n.max(1) as f64;

        doc_idx
    }

    pub fn search(&self, query: &str, top_k: usize) -> Vec<(usize, f64)> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() || self.n == 0 {
            return vec![];
        }

        let mut scores: Vec<(usize, f64)> = self
            .documents
            .iter()
            .enumerate()
            .map(|(idx, doc_tokens)| {
                let score = self.score_document(&query_tokens, doc_tokens, idx);
                (idx, score)
            })
            .filter(|(_, score)| *score > 0.0)
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(top_k);
        scores
    }

    fn score_document(
        &self,
        query_tokens: &[String],
        doc_tokens: &[String],
        doc_idx: usize,
    ) -> f64 {
        let doc_len = self.doc_lengths[doc_idx] as f64;
        let mut score = 0.0;

        for qt in query_tokens {
            let tf = doc_tokens.iter().filter(|t| *t == qt).count() as f64;
            if tf == 0.0 {
                continue;
            }

            let df = *self.df.get(qt).unwrap_or(&0) as f64;
            let idf = ((self.n as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();
            let numerator = tf * (self.k1 + 1.0);
            let denominator = tf + self.k1 * (1.0 - self.b + self.b * doc_len / self.avg_doc_length);
            score += idf * numerator / denominator;
        }

        score
    }
}

impl Default for BM25 {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Cosine similarity for embedding vectors
// ---------------------------------------------------------------------------

pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot = 0.0f64;
    let mut mag_a = 0.0f64;
    let mut mag_b = 0.0f64;

    for (x, y) in a.iter().zip(b.iter()) {
        let x = *x as f64;
        let y = *y as f64;
        dot += x * y;
        mag_a += x * x;
        mag_b += y * y;
    }

    let denom = mag_a.sqrt() * mag_b.sqrt();
    if denom == 0.0 {
        return 0.0;
    }

    (dot / denom) as f32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenization() {
        let tokens = tokenize("The quick brown fox jumps over the lazy dog");
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
    }

    #[test]
    fn bm25_search() {
        let mut engine = BM25::new();
        engine.add_document("Rust is a systems programming language");
        engine.add_document("Python is great for machine learning");
        engine.add_document("Rust and Python are both popular");
        engine.add_document("JavaScript runs in the browser");

        let results = engine.search("Rust programming", 2);
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 0);
    }

    #[test]
    fn cosine_sim() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 1e-6);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 1e-6);
    }
}
