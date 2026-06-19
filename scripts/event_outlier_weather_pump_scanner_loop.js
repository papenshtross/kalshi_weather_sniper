#!/usr/bin/env node
/*
 * Dashboard-controlled scanner for the event-outlier weather pump strategy.
 * By default it scans live data and writes paper intents. If the dashboard DB config
 * has both live_trading=true and live_launch_armed=true, it calls the guarded CLOB
 * executor after each scan. Start/stop is controlled by Postgres strategies.status
 * via scripts/arb_sniper_control_watchdog.js and systemd user service state.
 */
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const { Client } = require('/home/administrator/projects/polybot-dash/node_modules/pg');

const STRATEGY_ID = process.env.EVENT_OUTLIER_STRATEGY_ID || 'live_event_outlier_weather_pump_v1';
const PROJECT_DIR = process.env.EVENT_OUTLIER_PROJECT_DIR || '/home/administrator/projects/polymarket-event-outlier-research';
const SAFE_STRATEGY_ID = STRATEGY_ID.replace(/[^a-zA-Z0-9_.-]+/g, '_');
const CONFIG_PATH = process.env.EVENT_OUTLIER_CONFIG_PATH || path.join(PROJECT_DIR, `configs/weather_outlier_scanner.${SAFE_STRATEGY_ID}.yaml`);
const SCAN_JSON = process.env.EVENT_OUTLIER_SCAN_JSON || `reports/deployment/${SAFE_STRATEGY_ID}.weather_outlier_scan.json`;
const SIGNALS_JSON = process.env.EVENT_OUTLIER_SIGNALS_JSON || `reports/deployment/${SAFE_STRATEGY_ID}.weather_outlier_signals.json`;
const PAPER_LOG = process.env.EVENT_OUTLIER_PAPER_LOG || `reports/deployment/${SAFE_STRATEGY_ID}.paper_signal_log.jsonl`;
const DEFAULT_INTERVAL_SECONDS = 90;
const FORECAST_CACHE_DIR = process.env.PM_OUTLIER_FORECAST_CACHE_DIR || path.join(PROJECT_DIR, 'data/openmeteo_forecast_cache');

function stableJitterSeconds(value, modulo = 45) {
  let h = 0;
  for (const ch of String(value)) h = ((h * 31) + ch.charCodeAt(0)) >>> 0;
  return h % modulo;
}

function loadEnv(filePath) {
  const out = {};
  if (!fs.existsSync(filePath)) return out;
  for (const line of fs.readFileSync(filePath, 'utf8').split(/\r?\n/)) {
    if (!line || line.trim().startsWith('#') || !line.includes('=')) continue;
    const i = line.indexOf('=');
    const k = line.slice(0, i).trim();
    let v = line.slice(i + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    out[k] = v;
  }
  return out;
}

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function yamlScalar(value) {
  if (value === null || value === undefined || value === '') return 'null';
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  return JSON.stringify(String(value));
}
function writeScannerConfig(config) {
    const scan = {
    city_filter: config.city_filter ?? null,
    provider: config.provider || 'auto',
    max_pages: Number(config.max_pages ?? 2),
    max_markets: Number(config.max_markets ?? 120),
    max_entry_price: Number(config.max_entry_price ?? 0.02),
    min_model_edge: Number(config.min_model_edge ?? 0.05),
    min_model_probability: Number(config.min_model_probability ?? 0.08),
    max_notional_usdc: Number(config.max_notional_usdc ?? 10.0),
    model_bucket_near_c: Number(config.model_bucket_near_c ?? 0.0),
    primary_model: config.primary_model ?? null,
    min_models_inside: config.min_models_inside ?? null,
    max_model_spread_c: config.max_model_spread_c ?? null,
    consensus_models: Array.isArray(config.consensus_models) ? config.consensus_models.join(',') : (config.consensus_models ?? null),
  };
const execCfg = {
    order_type: config.order_type || 'FAK',
    side: 'BUY',
    outcome: 'YES',
    taker_fee_weather: Number(config.taker_fee_weather ?? 0.05),
    min_order_size_shares: Number(config.min_order_size_shares ?? 5),
    tick_size: Number(config.tick_size ?? 0.001),
  };
  const exitCfg = {
    take_profit_multiplier: Number(config.take_profit_multiplier ?? 3.0),
    stop_fraction_of_entry_threshold: Number(config.stop_fraction_of_entry_threshold ?? 0.5),
    max_hold_hours: Number(config.max_hold_hours ?? 24),
    prefer_exit_into_pump: true,
  };
  const lines = [
    '# Auto-generated from dashboard DB config. Paper/live-data scanner only; never live order placement.',
    'mode: paper',
    'live_trading: false',
    'require_explicit_confirmation_for_live: true',
    'scan:',
  ];
  for (const [k, v] of Object.entries(scan)) lines.push(`  ${k}: ${yamlScalar(v)}`);
  lines.push('execution_assumptions:');
  for (const [k, v] of Object.entries(execCfg)) lines.push(`  ${k}: ${yamlScalar(v)}`);
  lines.push('exit_rules:');
  for (const [k, v] of Object.entries(exitCfg)) lines.push(`  ${k}: ${yamlScalar(v)}`);
  fs.mkdirSync(path.dirname(CONFIG_PATH), { recursive: true });
  fs.writeFileSync(CONFIG_PATH, lines.join('\n') + '\n');
}

async function log(client, level, message) {
  const msg = String(message).slice(0, 1800);
  await client.query('INSERT INTO strategy_logs(strategy_id, ts, level, message) VALUES ($1, now(), $2, $3)', [STRATEGY_ID, level, msg]);
  console.log(`${new Date().toISOString()} ${level} ${msg}`);
}

(async () => {
  const env = {
    ...process.env,
    ...loadEnv('/home/administrator/projects/polybot/.env'),
    ...loadEnv('/home/administrator/projects/polybot/.env.live'),
    ...loadEnv('/home/administrator/projects/polybot-dash/.env.local'),
  };
  const url = env.POSTGRES_URL || env.DATABASE_URL || env.NAUTILUS_DB_URL;
  if (!url) throw new Error('missing POSTGRES_URL/DATABASE_URL');
  const client = new Client({ connectionString: url, ssl: url.includes('sslmode=require') ? { rejectUnauthorized: false } : undefined });
  await client.connect();
  try {
    await log(client, 'INFO', 'event-outlier weather pump scanner service started; live CLOB execution runs only when live_trading=true and live_launch_armed=true');
    while (true) {
      const res = await client.query('SELECT status, config FROM strategies WHERE id=$1', [STRATEGY_ID]);
      if (res.rowCount === 0) {
        await log(client, 'ERROR', 'strategy row missing; exiting scanner loop');
        process.exit(1);
      }
      const row = res.rows[0];
      const status = String(row.status || 'stopped');
      const cfg = row.config || {};
      if (status !== 'running') {
        await log(client, 'INFO', `strategy status=${status}; exiting scanner loop`);
        process.exit(0);
      }
      const liveTrading = cfg.live_trading === true || cfg.live_trading === 'true';
      const liveArmed = cfg.live_launch_armed === true || cfg.live_launch_armed === 'true';
      if (liveTrading && !liveArmed) {
        await log(client, 'ERROR', 'refusing live execution: live_trading=true but live_launch_armed is not true');
        process.exit(2);
      }
      writeScannerConfig(cfg);
      let stdout = '';
      try {
        const childEnv = {
          ...process.env,
          EVENT_OUTLIER_STRATEGY_ID: STRATEGY_ID,
          PM_OUTLIER_FORECAST_CACHE_DIR: FORECAST_CACHE_DIR,
          PM_OUTLIER_FORECAST_CACHE_TTL_SECONDS: String(cfg.forecast_cache_ttl_seconds || process.env.PM_OUTLIER_FORECAST_CACHE_TTL_SECONDS || 1800),
          PM_OUTLIER_FORECAST_429_COOLDOWN_SECONDS: String(cfg.forecast_429_cooldown_seconds || process.env.PM_OUTLIER_FORECAST_429_COOLDOWN_SECONDS || 600),
        };
        stdout = execFileSync(
          path.join(PROJECT_DIR, '.venv/bin/python'),
          ['scripts_weather_outlier_scanner.py', '--config', CONFIG_PATH, '--out', SCAN_JSON, '--export-signals', SIGNALS_JSON, '--paper-log', PAPER_LOG],
          { cwd: PROJECT_DIR, env: childEnv, encoding: 'utf8', timeout: Number(cfg.scan_timeout_ms || 240000), maxBuffer: 1024 * 1024 * 8 },
        );
        let candidates = 'unknown';
        let markets = 'unknown';
        try {
          const full = JSON.parse(fs.readFileSync(path.join(PROJECT_DIR, SCAN_JSON), 'utf8'));
          candidates = full.candidates ?? (full.rows || []).filter(r => r.candidate).length;
          markets = full.markets_scanned ?? full.markets_scanned_count ?? 'unknown';
        } catch {}
        const exitOnlyMode = cfg.exit_only_mode === true || cfg.exit_only_mode === 'true' || cfg.entry_orders_enabled === false || cfg.entry_orders_enabled === 'false' || Number(cfg.max_live_orders_per_scan || 0) <= 0;
        await log(client, Number(candidates) > 0 && !exitOnlyMode ? 'WARN' : 'INFO', `scan complete: candidates=${candidates}, markets_scanned=${markets}, provider=${cfg.provider || 'auto'}, max_entry_price=${cfg.max_entry_price ?? 0.02}, min_model_edge=${cfg.min_model_edge ?? 0.05}; reports=${SCAN_JSON}; exit_only_mode=${exitOnlyMode}`);
        if (liveTrading && liveArmed) {
          if (exitOnlyMode) {
            const exitOut = execFileSync(
              '/home/administrator/projects/polybot/.venv/bin/python',
              ['/home/administrator/projects/polybot/scripts/tmp_event_outlier_exit_only_once.py'],
              { cwd: '/home/administrator/projects/polybot', env: childEnv, encoding: 'utf8', timeout: Number(cfg.live_execution_timeout_ms || 120000), maxBuffer: 1024 * 1024 * 4 },
            );
            await log(client, 'INFO', `exit-only executor result: ${exitOut.trim().slice(-1200)}`);
          } else {
            const execOut = execFileSync(
              '/home/administrator/projects/polybot/.venv/bin/python',
              ['/home/administrator/projects/polybot/scripts/event_outlier_live_executor.py', '--strategy-id', STRATEGY_ID, '--signals', path.join(PROJECT_DIR, SIGNALS_JSON)],
              { cwd: '/home/administrator/projects/polybot', env: childEnv, encoding: 'utf8', timeout: Number(cfg.live_execution_timeout_ms || 120000), maxBuffer: 1024 * 1024 * 4 },
            );
            await log(client, 'INFO', `live executor result: ${execOut.trim().slice(-1200)}`);
            const exitOut = execFileSync(
              '/home/administrator/projects/polybot/.venv/bin/python',
              ['/home/administrator/projects/polybot/scripts/tmp_event_outlier_exit_only_once.py'],
              { cwd: '/home/administrator/projects/polybot', env: childEnv, encoding: 'utf8', timeout: Number(cfg.live_execution_timeout_ms || 120000), maxBuffer: 1024 * 1024 * 4 },
            );
            await log(client, 'INFO', `post-buy exit monitor result: ${exitOut.trim().slice(-1200)}`);
          }
        }
      } catch (err) {
        const details = (err.stdout || stdout || err.stderr || err.message || String(err)).toString().slice(-1200);
        await log(client, 'ERROR', `scanner run failed: ${details}`);
      }
      const interval = Math.max(15, Number(cfg.scan_interval_seconds || DEFAULT_INTERVAL_SECONDS));
      const jitter = stableJitterSeconds(STRATEGY_ID, Number(cfg.scan_jitter_seconds || 45));
      await sleep((interval + jitter) * 1000);
    }
  } finally {
    await client.end();
  }
})().catch((err) => {
  console.error(err.stack || err.message || err);
  process.exit(1);
});
