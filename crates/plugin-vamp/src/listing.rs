//! Render a single parody-coin listing as a standalone HTML file.

use std::path::Path;

use anyhow::{Context as _, Result};
use html_escape::encode_safe;

use crate::state::GeneratedCoin;

pub fn render(coin: &GeneratedCoin) -> String {
    let name = encode_safe(&coin.name);
    let ticker = encode_safe(&coin.ticker);
    let blurb = encode_safe(&coin.blurb);
    let website = encode_safe(&coin.website);
    let src_name = encode_safe(&coin.source_coin.name);
    let src_symbol = encode_safe(&coin.source_coin.symbol);
    let src_url = encode_safe(&coin.source_coin.url);
    let created = coin.created_at.to_rfc3339();

    let tweet_block = match coin.source_trend.as_ref().and_then(|t| t.top_tweet.as_ref()) {
        Some(t) => {
            let url = encode_safe(&t.url);
            let author = encode_safe(&t.author);
            let text = encode_safe(&t.text);
            format!(
                r##"
    <section class="tweet">
      <div class="tweet-head">paired tweet · @{author}</div>
      <p class="tweet-body">{text}</p>
      <a class="tweet-link" href="{url}" target="_blank" rel="noopener">{url}</a>
    </section>"##
            )
        }
        None => String::new(),
    };

    let trend_line = coin
        .source_trend
        .as_ref()
        .map(|t| format!("inspired by trending topic <em>{}</em>", encode_safe(&t.name)))
        .unwrap_or_default();

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${ticker} — {name}</title>
<style>
:root {{
  --bg:#0b0b14; --panel:#15151f; --border:#22223a;
  --text:#e8e8f0; --dim:#9999b5; --accent:#ec4899; --accent2:#a78bfa; --green:#10b981;
}}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; background:var(--bg); color:var(--text); min-height:100vh; }}
main {{ max-width:720px; margin:0 auto; padding:72px 24px 96px; }}
.parody-tag {{ display:inline-block; font-size:11px; letter-spacing:0.18em; text-transform:uppercase; color:var(--dim); border:1px solid var(--border); padding:4px 10px; border-radius:999px; }}
h1 {{ font-size:64px; line-height:1.05; margin:20px 0 8px; letter-spacing:-0.03em; }}
h1 .ticker {{ background:linear-gradient(135deg,var(--accent),var(--accent2)); -webkit-background-clip:text; background-clip:text; color:transparent; }}
.subtitle {{ color:var(--dim); font-size:18px; margin-bottom:32px; }}
.blurb {{ font-size:18px; line-height:1.6; background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:24px; margin:24px 0; }}
.meta {{ display:grid; grid-template-columns:1fr 1fr; gap:12px; margin:24px 0; }}
.meta .cell {{ background:var(--panel); border:1px solid var(--border); padding:14px 16px; border-radius:10px; }}
.meta .label {{ font-size:11px; letter-spacing:0.12em; text-transform:uppercase; color:var(--dim); }}
.meta .value {{ font-size:15px; margin-top:4px; word-break:break-all; }}
.tweet {{ background:var(--panel); border:1px solid var(--border); border-radius:12px; padding:20px 22px; margin:24px 0; }}
.tweet-head {{ color:var(--dim); font-size:12px; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:8px; }}
.tweet-body {{ font-size:16px; line-height:1.5; margin-bottom:10px; }}
.tweet-link {{ color:var(--accent2); font-size:12px; text-decoration:none; word-break:break-all; }}
.actions {{ display:flex; gap:12px; margin-top:32px; flex-wrap:wrap; }}
.button {{ display:inline-block; padding:12px 18px; border-radius:10px; text-decoration:none; font-weight:600; font-size:14px; }}
.primary {{ background:var(--accent); color:#fff; }}
.secondary {{ background:var(--panel); color:var(--text); border:1px solid var(--border); }}
.footer {{ color:var(--dim); font-size:12px; margin-top:48px; line-height:1.6; }}
.footer a {{ color:var(--accent2); }}
em {{ color:var(--text); font-style:normal; font-weight:600; }}
</style>
</head>
<body>
<main>
  <span class="parody-tag">parody · not financial advice</span>
  <h1>{name} <span class="ticker">${ticker}</span></h1>
  <p class="subtitle">A pseudo-vamp of <strong>{src_name}</strong> ({src_symbol}). {trend_line}</p>

  <div class="blurb">{blurb}</div>

  <div class="meta">
    <div class="cell">
      <div class="label">vamped coin</div>
      <div class="value">{src_name} ({src_symbol})</div>
    </div>
    <div class="cell">
      <div class="label">source chart</div>
      <div class="value"><a href="{src_url}" target="_blank" rel="noopener">{src_url}</a></div>
    </div>
  </div>

  {tweet_block}

  <div class="actions">
    <a class="button primary" href="https://{website}" target="_blank" rel="noopener">visit {website}</a>
    <a class="button secondary" href="{src_url}" target="_blank" rel="noopener">view original on dexscreener</a>
  </div>

  <p class="footer">
    Generated {created} by rustliza-vamp. This is a parody concept — no token has been minted, no liquidity exists, and no one is selling you anything.
  </p>
</main>
</body>
</html>"##
    )
}

pub fn write_to(dir: impl AsRef<Path>, coin: &GeneratedCoin) -> Result<std::path::PathBuf> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir).with_context(|| format!("create dir {}", dir.display()))?;
    let filename = format!(
        "{}.html",
        sanitize_filename(&coin.ticker.to_lowercase())
    );
    let path = dir.join(filename);
    std::fs::write(&path, render(coin))
        .with_context(|| format!("write listing {}", path.display()))?;
    Ok(path)
}

fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}
