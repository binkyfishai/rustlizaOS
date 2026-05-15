//! HTTP surface for the pseudo-vamp coin generator.
//!
//! Routes mounted under `/vamp/*`:
//! * `GET  /vamp`          — the SPA
//! * `POST /vamp/run`      — generate one coin synchronously, return JSON
//! * `GET  /vamp/coins`    — list previously-generated coins
//! * `GET  /vamp/config`   — current auto-mode + interval state
//! * `POST /vamp/config`   — update auto-mode + interval state
//! * `GET  /vamp/listings/{ticker}` — serve a generated listing HTML
//! * `GET  /vamp/trends`   — current curated trends (for the UI sidebar)

use axum::extract::{Path, State as AxumState};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use rustliza_plugin_vamp::{GeneratedCoin, ScrapedTrend, VampConfig};

use crate::{ApiState, ErrorResponse};

// ---------------------------------------------------------------------------
// JSON payloads
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct CoinView {
    id: String,
    created_at: String,
    name: String,
    ticker: String,
    blurb: String,
    website: String,
    source_name: String,
    source_symbol: String,
    source_chain: String,
    source_url: String,
    trend: Option<String>,
    tweet_url: Option<String>,
    tweet_author: Option<String>,
    tweet_text: Option<String>,
    listing_url: String,
}

impl From<&GeneratedCoin> for CoinView {
    fn from(c: &GeneratedCoin) -> Self {
        let listing_url = format!("/vamp/listings/{}", c.ticker.to_lowercase());
        let (trend, tweet_url, tweet_author, tweet_text) = match &c.source_trend {
            Some(t) => (
                Some(t.name.clone()),
                t.top_tweet.as_ref().map(|tw| tw.url.clone()),
                t.top_tweet.as_ref().map(|tw| tw.author.clone()),
                t.top_tweet.as_ref().map(|tw| tw.text.clone()),
            ),
            None => (None, None, None, None),
        };
        Self {
            id: c.id.to_string(),
            created_at: c.created_at.to_rfc3339(),
            name: c.name.clone(),
            ticker: c.ticker.clone(),
            blurb: c.blurb.clone(),
            website: c.website.clone(),
            source_name: c.source_coin.name.clone(),
            source_symbol: c.source_coin.symbol.clone(),
            source_chain: c.source_coin.chain.clone(),
            source_url: c.source_coin.url.clone(),
            trend,
            tweet_url,
            tweet_author,
            tweet_text,
            listing_url,
        }
    }
}

#[derive(Serialize)]
struct TrendView {
    name: String,
    category: Option<String>,
    post_count: Option<String>,
    tweet_url: Option<String>,
    scraped_at: String,
}

impl From<&ScrapedTrend> for TrendView {
    fn from(t: &ScrapedTrend) -> Self {
        Self {
            name: t.name.clone(),
            category: t.category.clone(),
            post_count: t.post_count.clone(),
            tweet_url: t.top_tweet.as_ref().map(|tw| tw.url.clone()),
            scraped_at: t.scraped_at.to_rfc3339(),
        }
    }
}

#[derive(Serialize)]
struct ConfigResponse {
    auto_enabled: bool,
    interval_secs: u64,
    scrape_enabled: bool,
    scrape_interval_secs: u64,
    coin_chain: String,
    max_trends: usize,
    last_scrape: Option<String>,
    last_generate: Option<String>,
    coin_count: usize,
    trend_count: usize,
    has_state: bool,
}

#[derive(Deserialize)]
struct ConfigUpdate {
    auto_enabled: Option<bool>,
    interval_secs: Option<u64>,
    scrape_enabled: Option<bool>,
    scrape_interval_secs: Option<u64>,
    coin_chain: Option<String>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

fn vamp_disabled() -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: "vamp plugin not enabled in this build".into(),
        }),
    )
}

async fn run(
    AxumState(state): AxumState<ApiState>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(vamp) = state.vamp.clone() else {
        return Err(vamp_disabled());
    };
    let runtime = state.runtime.clone();
    let coin = rustliza_plugin_vamp::generate_now(vamp, runtime.as_ref())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;
    Ok(Json(CoinView::from(&coin)))
}

async fn list_coins(
    AxumState(state): AxumState<ApiState>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(vamp) = state.vamp.clone() else {
        return Err(vamp_disabled());
    };
    let coins = vamp.coins().await;
    let view: Vec<CoinView> = coins.iter().rev().map(CoinView::from).collect();
    Ok(Json(view))
}

async fn list_trends(
    AxumState(state): AxumState<ApiState>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(vamp) = state.vamp.clone() else {
        return Err(vamp_disabled());
    };
    let trends = vamp.trends().await;
    let view: Vec<TrendView> = trends.iter().map(TrendView::from).collect();
    Ok(Json(view))
}

async fn get_config(
    AxumState(state): AxumState<ApiState>,
) -> impl IntoResponse {
    let Some(vamp) = state.vamp.clone() else {
        return Json(ConfigResponse {
            auto_enabled: false,
            interval_secs: 0,
            scrape_enabled: false,
            scrape_interval_secs: 0,
            coin_chain: String::new(),
            max_trends: 0,
            last_scrape: None,
            last_generate: None,
            coin_count: 0,
            trend_count: 0,
            has_state: false,
        });
    };

    let cfg = vamp.config().await;
    let last_scrape = vamp.last_scrape().await.map(|d| d.to_rfc3339());
    let last_generate = vamp.last_generate().await.map(|d| d.to_rfc3339());
    let coin_count = vamp.coins().await.len();
    let trend_count = vamp.trends().await.len();

    Json(config_response(&cfg, last_scrape, last_generate, coin_count, trend_count))
}

fn config_response(
    cfg: &VampConfig,
    last_scrape: Option<String>,
    last_generate: Option<String>,
    coin_count: usize,
    trend_count: usize,
) -> ConfigResponse {
    ConfigResponse {
        auto_enabled: cfg.auto_enabled,
        interval_secs: cfg.interval_secs,
        scrape_enabled: cfg.scrape_enabled,
        scrape_interval_secs: cfg.scrape_interval_secs,
        coin_chain: cfg.coin_chain.clone(),
        max_trends: cfg.max_trends,
        last_scrape,
        last_generate,
        coin_count,
        trend_count,
        has_state: true,
    }
}

async fn update_config(
    AxumState(state): AxumState<ApiState>,
    Json(req): Json<ConfigUpdate>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(vamp) = state.vamp.clone() else {
        return Err(vamp_disabled());
    };
    let cfg = vamp
        .update_config(|c| {
            if let Some(v) = req.auto_enabled {
                c.auto_enabled = v;
            }
            if let Some(v) = req.interval_secs {
                c.interval_secs = v;
            }
            if let Some(v) = req.scrape_enabled {
                c.scrape_enabled = v;
            }
            if let Some(v) = req.scrape_interval_secs {
                c.scrape_interval_secs = v;
            }
            if let Some(v) = req.coin_chain {
                c.coin_chain = v;
            }
        })
        .await;

    let last_scrape = vamp.last_scrape().await.map(|d| d.to_rfc3339());
    let last_generate = vamp.last_generate().await.map(|d| d.to_rfc3339());
    let coin_count = vamp.coins().await.len();
    let trend_count = vamp.trends().await.len();

    Ok(Json(config_response(
        &cfg,
        last_scrape,
        last_generate,
        coin_count,
        trend_count,
    )))
}

async fn serve_listing(
    AxumState(state): AxumState<ApiState>,
    Path(ticker): Path<String>,
) -> std::result::Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(vamp) = state.vamp.clone() else {
        return Err(vamp_disabled());
    };
    let want = ticker.to_lowercase();
    let coins = vamp.coins().await;
    let Some(coin) = coins.iter().rev().find(|c| c.ticker.eq_ignore_ascii_case(&want)) else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("no generated coin with ticker {ticker}"),
            }),
        ));
    };
    let html = rustliza_plugin_vamp::listing::render(coin);
    Ok(Html(html))
}

async fn ui() -> impl IntoResponse {
    Html(VAMP_HTML)
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/vamp", get(ui))
        .route("/vamp/run", post(run))
        .route("/vamp/coins", get(list_coins))
        .route("/vamp/trends", get(list_trends))
        .route("/vamp/config", get(get_config).post(update_config))
        .route("/vamp/listings/{ticker}", get(serve_listing))
}

// ---------------------------------------------------------------------------
// SPA
// ---------------------------------------------------------------------------

const VAMP_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>pseudovamp</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0a0a14;--panel:#15151f;--panel-2:#1c1c2a;--border:#26263a;
  --text:#e8e8f0;--dim:#8a8aa6;--dim2:#b6b6cf;
  --accent:#ec4899;--accent2:#a78bfa;--green:#10b981;--red:#ef4444;--yellow:#eab308;
}
html,body{height:100%}
body{font-family:'SF Pro Text','Inter',-apple-system,BlinkMacSystemFont,sans-serif;background:var(--bg);color:var(--text);font-size:13px;line-height:1.5}
.mono{font-family:'SF Mono','JetBrains Mono',Monaco,monospace}
a{color:var(--accent2);text-decoration:none}
a:hover{text-decoration:underline}

#app{display:grid;grid-template-rows:48px 1fr;height:100vh}
#topbar{display:flex;align-items:center;padding:0 18px;gap:14px;background:var(--panel);border-bottom:1px solid var(--border);font-size:12px}
#topbar .logo{font-weight:800;font-size:15px;letter-spacing:-0.02em;background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}
#topbar .tagline{color:var(--dim);font-size:11px;letter-spacing:0.08em;text-transform:uppercase}
#topbar .sep{width:1px;height:20px;background:var(--border)}
#topbar .pill{padding:5px 10px;border-radius:5px;background:var(--panel-2);color:var(--dim2);font-family:'SF Mono',monospace;font-size:11px;display:flex;align-items:center;gap:6px}
#topbar .pill.live .dot{background:var(--green)}
#topbar .pill .dot{width:6px;height:6px;border-radius:50%;background:var(--dim)}
#topbar .spacer{flex:1}

#main{display:grid;grid-template-columns:300px 1fr;height:calc(100vh - 48px);overflow:hidden}

#left{background:var(--panel);border-right:1px solid var(--border);overflow-y:auto;padding:18px}
#left h2{font-size:11px;text-transform:uppercase;letter-spacing:0.1em;color:var(--dim);margin-bottom:12px;font-weight:700}

.section{margin-bottom:24px}
.kv{display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border)}
.kv:last-child{border:none}
.kv .k{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:0.06em}
.kv .v{color:var(--text);font-family:'SF Mono',monospace;font-size:12px}

.controls{display:flex;flex-direction:column;gap:10px}
.row{display:flex;gap:8px;align-items:center}
label{font-size:12px;color:var(--dim2)}
input[type=text],input[type=number]{background:var(--panel-2);border:1px solid var(--border);color:var(--text);padding:7px 10px;border-radius:5px;font-size:12px;font-family:inherit;outline:none;width:100%}
input[type=text]:focus,input[type=number]:focus{border-color:var(--accent)}
.toggle{position:relative;width:38px;height:20px;background:var(--panel-2);border-radius:10px;cursor:pointer;border:1px solid var(--border)}
.toggle.on{background:var(--accent);border-color:var(--accent)}
.toggle .nub{position:absolute;top:1px;left:1px;width:16px;height:16px;background:#fff;border-radius:50%;transition:left .15s ease}
.toggle.on .nub{left:19px}

button{background:var(--accent);color:#fff;border:none;padding:9px 14px;border-radius:6px;font-size:12px;cursor:pointer;font-family:inherit;font-weight:600}
button:hover{opacity:.9}
button:disabled{opacity:.4;cursor:default}
button.ghost{background:var(--panel-2);border:1px solid var(--border);color:var(--text)}

#trends{display:flex;flex-direction:column;gap:6px}
.trend{padding:8px 10px;background:var(--panel-2);border:1px solid var(--border);border-radius:6px;font-size:12px}
.trend .t-name{font-weight:600;color:var(--text)}
.trend .t-meta{font-size:10px;color:var(--dim);margin-top:2px;font-family:'SF Mono',monospace}

#right{overflow-y:auto;padding:22px 28px;background:var(--bg)}
#right h1{font-size:18px;font-weight:700;margin-bottom:4px}
#right .sub{font-size:12px;color:var(--dim);margin-bottom:22px}

#coins{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:14px}
.empty{padding:40px;text-align:center;color:var(--dim);background:var(--panel);border:1px dashed var(--border);border-radius:10px}

.coin{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px 18px;display:flex;flex-direction:column;gap:10px;transition:border-color .15s ease}
.coin:hover{border-color:var(--accent2)}
.coin .head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.coin .title{font-size:18px;font-weight:700;line-height:1.2}
.coin .ticker{display:inline-block;font-family:'SF Mono',monospace;font-size:11px;padding:3px 7px;border-radius:4px;background:#ec489922;color:var(--accent);font-weight:700}
.coin .blurb{font-size:13px;line-height:1.5;color:var(--dim2)}
.coin .meta{display:flex;flex-wrap:wrap;gap:10px;font-size:11px;color:var(--dim);font-family:'SF Mono',monospace;border-top:1px solid var(--border);padding-top:10px}
.coin .meta .label{color:var(--dim)}
.coin .meta .val{color:var(--text)}
.coin .tweet{background:var(--panel-2);border:1px solid var(--border);border-radius:6px;padding:10px 12px;font-size:12px}
.coin .tweet .head2{font-size:10px;text-transform:uppercase;letter-spacing:0.08em;color:var(--dim);margin-bottom:4px}
.coin .tweet .text{color:var(--dim2);line-height:1.4}
.coin .actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:auto}
.coin .actions a{font-size:11px;padding:5px 9px;border-radius:5px;background:var(--panel-2);border:1px solid var(--border);color:var(--text)}
.coin .actions a:hover{border-color:var(--accent);text-decoration:none}

#runbar{display:flex;align-items:center;gap:10px;margin-bottom:22px;padding:14px 18px;background:var(--panel);border:1px solid var(--border);border-radius:10px}
#runbar .label{font-size:11px;text-transform:uppercase;letter-spacing:0.08em;color:var(--dim);font-weight:700}
#runbar .spacer{flex:1}
#status{font-size:12px;color:var(--dim);font-family:'SF Mono',monospace}
#status.err{color:var(--red)}
#status.ok{color:var(--green)}

</style>
</head>
<body>
<div id="app">
  <div id="topbar">
    <div class="logo">▮ pseudovamp</div>
    <div class="tagline">parody coin generator</div>
    <div class="sep"></div>
    <div class="pill" id="agent-pill"><span class="dot"></span><span id="agent-name">connecting</span></div>
    <div class="pill" id="auto-pill"><span class="dot"></span><span id="auto-label">auto: off</span></div>
    <div class="spacer"></div>
    <div class="pill mono" id="counts">0 coins · 0 trends</div>
  </div>

  <div id="main">
    <div id="left">
      <div class="section">
        <h2>state</h2>
        <div class="kv"><span class="k">last scrape</span><span class="v" id="kv-scrape">—</span></div>
        <div class="kv"><span class="k">last mint</span><span class="v" id="kv-gen">—</span></div>
        <div class="kv"><span class="k">chain</span><span class="v" id="kv-chain">—</span></div>
      </div>

      <div class="section">
        <h2>controls</h2>
        <div class="controls">
          <div class="row">
            <label style="flex:1">auto-mint</label>
            <div class="toggle" id="auto-toggle"><div class="nub"></div></div>
          </div>
          <div class="row">
            <label style="flex:1">mint every (sec)</label>
            <input type="number" id="interval" min="5" step="1" style="width:90px">
          </div>
          <div class="row">
            <label style="flex:1">scrape trends</label>
            <div class="toggle" id="scrape-toggle"><div class="nub"></div></div>
          </div>
          <div class="row">
            <label style="flex:1">scrape every (sec)</label>
            <input type="number" id="scrape-interval" min="60" step="1" style="width:90px">
          </div>
          <div class="row">
            <label style="flex:1">chain</label>
            <input type="text" id="chain" style="width:120px">
          </div>
          <button class="ghost" id="save">save config</button>
        </div>
      </div>

      <div class="section">
        <h2>current x trends</h2>
        <div id="trends"><div class="empty" style="padding:20px;font-size:12px">no trends yet</div></div>
      </div>
    </div>

    <div id="right">
      <h1>generated coins</h1>
      <div class="sub">parody concepts. no token has been deployed. nothing is for sale.</div>

      <div id="runbar">
        <span class="label">manual</span>
        <button id="run">mint one now</button>
        <div class="spacer"></div>
        <span id="status">idle</span>
      </div>

      <div id="coins"><div class="empty">click <strong>mint one now</strong> to generate your first parody coin.</div></div>
    </div>
  </div>
</div>

<script>
const $ = s => document.querySelector(s);
const $$ = s => [...document.querySelectorAll(s)];

let cfgLoaded = false;

function fmtTs(s) {
  if (!s) return '—';
  try { return new Date(s).toLocaleTimeString(); } catch(e) { return s; }
}

function escapeHtml(s) {
  return (s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

async function loadAgent() {
  try {
    const r = await fetch('/agent');
    const j = await r.json();
    $('#agent-name').textContent = j.name;
    $('#agent-pill').classList.add('live');
  } catch(e) {
    $('#agent-name').textContent = 'offline';
  }
}

async function loadConfig() {
  const r = await fetch('/vamp/config');
  const j = await r.json();
  if (!j.has_state) {
    $('#status').textContent = 'vamp plugin not enabled — start binary with vamp plugin';
    $('#status').className = 'err';
    $('#run').disabled = true;
    return;
  }
  $('#kv-scrape').textContent = fmtTs(j.last_scrape);
  $('#kv-gen').textContent = fmtTs(j.last_generate);
  $('#kv-chain').textContent = j.coin_chain;
  $('#counts').textContent = `${j.coin_count} coins · ${j.trend_count} trends`;
  $('#auto-toggle').classList.toggle('on', !!j.auto_enabled);
  $('#scrape-toggle').classList.toggle('on', !!j.scrape_enabled);
  $('#auto-label').textContent = 'auto: ' + (j.auto_enabled ? 'on' : 'off');
  if (!cfgLoaded) {
    $('#interval').value = j.interval_secs;
    $('#scrape-interval').value = j.scrape_interval_secs;
    $('#chain').value = j.coin_chain;
    cfgLoaded = true;
  }
}

async function loadTrends() {
  const r = await fetch('/vamp/trends');
  const j = await r.json();
  const c = $('#trends');
  if (!j.length) {
    c.innerHTML = '<div class="empty" style="padding:20px;font-size:12px">no trends yet — wait for first scrape</div>';
    return;
  }
  c.innerHTML = j.map(t => `
    <div class="trend">
      <div class="t-name">${escapeHtml(t.name)}</div>
      <div class="t-meta">${escapeHtml(t.category || '')}${t.post_count ? ' · ' + escapeHtml(t.post_count) : ''}${t.tweet_url ? ' · <a href="' + escapeHtml(t.tweet_url) + '" target="_blank">tweet</a>' : ''}</div>
    </div>
  `).join('');
}

function coinCard(c) {
  const tweetBlock = c.tweet_url ? `
    <div class="tweet">
      <div class="head2">paired tweet · @${escapeHtml(c.tweet_author || 'anon')}</div>
      <div class="text">${escapeHtml(c.tweet_text || '')}</div>
    </div>` : '';

  const trendChip = c.trend ? `<span class="val">${escapeHtml(c.trend)}</span>` : '<span class="val">—</span>';

  return `
    <div class="coin">
      <div class="head">
        <div class="title">${escapeHtml(c.name)}</div>
        <div class="ticker">$${escapeHtml(c.ticker)}</div>
      </div>
      <div class="blurb">${escapeHtml(c.blurb)}</div>
      ${tweetBlock}
      <div class="meta">
        <div><span class="label">vamped:</span> <span class="val">${escapeHtml(c.source_name)} (${escapeHtml(c.source_symbol)})</span></div>
        <div><span class="label">trend:</span> ${trendChip}</div>
        <div><span class="label">site:</span> <span class="val">${escapeHtml(c.website)}</span></div>
      </div>
      <div class="actions">
        <a href="${escapeHtml(c.listing_url)}" target="_blank">view listing →</a>
        <a href="${escapeHtml(c.source_url)}" target="_blank">source chart</a>
        ${c.tweet_url ? `<a href="${escapeHtml(c.tweet_url)}" target="_blank">tweet</a>` : ''}
        <a href="https://${escapeHtml(c.website)}" target="_blank">${escapeHtml(c.website)}</a>
      </div>
    </div>
  `;
}

async function loadCoins() {
  const r = await fetch('/vamp/coins');
  const j = await r.json();
  const c = $('#coins');
  if (!j.length) {
    c.innerHTML = '<div class="empty">click <strong>mint one now</strong> to generate your first parody coin.</div>';
    return;
  }
  c.innerHTML = j.map(coinCard).join('');
}

async function mintOne() {
  $('#run').disabled = true;
  $('#status').textContent = 'minting…';
  $('#status').className = '';
  try {
    const r = await fetch('/vamp/run', {method:'POST'});
    if (!r.ok) {
      const err = await r.json().catch(() => ({error:r.statusText}));
      throw new Error(err.error || r.statusText);
    }
    const c = await r.json();
    $('#status').textContent = 'minted $' + c.ticker;
    $('#status').className = 'ok';
    await Promise.all([loadCoins(), loadConfig()]);
  } catch(e) {
    $('#status').textContent = 'failed: ' + e.message;
    $('#status').className = 'err';
  } finally {
    $('#run').disabled = false;
  }
}

async function saveConfig() {
  const body = {
    auto_enabled: $('#auto-toggle').classList.contains('on'),
    scrape_enabled: $('#scrape-toggle').classList.contains('on'),
    interval_secs: parseInt($('#interval').value, 10),
    scrape_interval_secs: parseInt($('#scrape-interval').value, 10),
    coin_chain: $('#chain').value.trim()
  };
  $('#status').textContent = 'saving config…';
  try {
    const r = await fetch('/vamp/config', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
    if (!r.ok) throw new Error(r.statusText);
    $('#status').textContent = 'config saved';
    $('#status').className = 'ok';
    await loadConfig();
  } catch(e) {
    $('#status').textContent = 'save failed: ' + e.message;
    $('#status').className = 'err';
  }
}

$('#run').addEventListener('click', mintOne);
$('#auto-toggle').addEventListener('click', () => $('#auto-toggle').classList.toggle('on'));
$('#scrape-toggle').addEventListener('click', () => $('#scrape-toggle').classList.toggle('on'));
$('#save').addEventListener('click', saveConfig);

(async () => {
  await loadAgent();
  await loadConfig();
  await loadTrends();
  await loadCoins();
  setInterval(loadConfig, 5000);
  setInterval(loadTrends, 15000);
  setInterval(loadCoins, 5000);
})();
</script>
</body>
</html>"##;
