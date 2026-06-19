#!/usr/bin/env node
/*
 * Mirrors the dashboard Postgres status for the dedicated arb sniper into the
 * local WSL systemd user service. The dashboard runs on Vercel and cannot call
 * systemctl directly, so Start/Stop writes DB state and this local watchdog is
 * the control-plane bridge.
 */
const fs = require('fs');
const { execFileSync } = require('child_process');
const { Client } = require('/home/administrator/projects/polybot-dash/node_modules/pg');

const DEFAULT_MAPPINGS = [
  { id: 'live_arb_sniper_btc15m_v1', service: 'polybot-arb-sniper.service' },
  { id: 'live_crypto_arb_sniper_btc15m_v1', service: 'polybot-crypto-arb-sniper-btc15m.service' },
  { id: 'live_crypto_arb_sniper_sol15m_v1', service: 'polybot-crypto-arb-sniper-sol15m.service' },
  { id: 'live_crypto_arb_sniper_sol5m_v1', service: 'polybot-crypto-arb-sniper-sol5m.service' },
  { id: 'live_crypto_arb_sniper_eth15m_v1', service: 'polybot-crypto-arb-sniper-eth15m.service' },
  { id: 'live_crypto_arb_sniper_eth5m_v1', service: 'polybot-crypto-arb-sniper-eth5m.service' },
  { id: 'live_crypto_arb_sniper_btc5m_v1', service: 'polybot-crypto-arb-sniper-btc5m.service' },
  { id: 'crypto_passive_mm_btc_15m', service: 'polybot-crypto-passive-mm-btc15m.service' },
  { id: 'live_event_outlier_weather_pump_v1', service: 'polybot-event-outlier-weather-pump-scanner.service' },
  { id: 'live_weather_arb_sniper_austin_20260429_v1', service: 'polybot-weather-arb-sniper.service' },
  { id: 'live_weather_arb_sniper_chicago_auto_v1', service: 'polybot-weather-arb-sniper-chicago.service' },
  { id: 'live_weather_arb_sniper_denver_auto_v1', service: 'polybot-weather-arb-sniper-denver.service' },
  { id: 'live_weather_arb_sniper_hong_kong_auto_v1', service: 'polybot-weather-arb-sniper-hong-kong.service' },
  { id: 'live_weather_arb_sniper_london_auto_v1', service: 'polybot-weather-arb-sniper-london.service' },
  { id: 'live_weather_arb_sniper_lucknow_auto_v1', service: 'polybot-weather-arb-sniper-lucknow.service' },
  { id: 'live_weather_arb_sniper_mexico_city_auto_v1', service: 'polybot-weather-arb-sniper-mexico-city.service' },
  { id: 'live_weather_arb_sniper_seoul_auto_v1', service: 'polybot-weather-arb-sniper-seoul.service' },
  { id: 'live_weather_arb_sniper_shanghai_auto_v1', service: 'polybot-weather-arb-sniper-shanghai.service' },
  { id: 'live_weather_arb_sniper_tokyo_auto_v1', service: 'polybot-weather-arb-sniper-tokyo.service' },
  { id: 'live_weather_arb_sniper_wellington_auto_v1', service: 'polybot-weather-arb-sniper-wellington.service' },
  { id: 'live_weather_outlier_sniper_austin_auto_v1', service: 'polybot-weather-outlier-sniper-austin.service' },
  { id: 'live_weather_outlier_sniper_chicago_auto_v1', service: 'polybot-weather-outlier-sniper-chicago.service' },
  { id: 'live_weather_outlier_sniper_denver_auto_v1', service: 'polybot-weather-outlier-sniper-denver.service' },
  { id: 'live_weather_outlier_sniper_hong_kong_auto_v1', service: 'polybot-weather-outlier-sniper-hong-kong.service' },
  { id: 'live_weather_outlier_sniper_london_auto_v1', service: 'polybot-weather-outlier-sniper-london.service' },
  { id: 'live_weather_outlier_sniper_lucknow_auto_v1', service: 'polybot-weather-outlier-sniper-lucknow.service' },
  { id: 'live_weather_outlier_sniper_mexico_city_auto_v1', service: 'polybot-weather-outlier-sniper-mexico-city.service' },
  { id: 'live_weather_outlier_sniper_seoul_auto_v1', service: 'polybot-weather-outlier-sniper-seoul.service' },
  { id: 'live_weather_outlier_sniper_shanghai_auto_v1', service: 'polybot-weather-outlier-sniper-shanghai.service' },
  { id: 'live_weather_outlier_sniper_tokyo_auto_v1', service: 'polybot-weather-outlier-sniper-tokyo.service' },
  { id: 'live_weather_outlier_sniper_wellington_auto_v1', service: 'polybot-weather-outlier-sniper-wellington.service' },
  { id: 'live_weather_outlier_sniper_amsterdam_auto_v1', service: 'polybot-weather-outlier-sniper-amsterdam.service' },
  { id: 'live_weather_outlier_sniper_ankara_auto_v1', service: 'polybot-weather-outlier-sniper-ankara.service' },
  { id: 'live_weather_outlier_sniper_atlanta_auto_v1', service: 'polybot-weather-outlier-sniper-atlanta.service' },
  { id: 'live_weather_outlier_sniper_beijing_auto_v1', service: 'polybot-weather-outlier-sniper-beijing.service' },
  { id: 'live_weather_outlier_sniper_buenos_aires_auto_v1', service: 'polybot-weather-outlier-sniper-buenos-aires.service' },
  { id: 'live_weather_outlier_sniper_busan_auto_v1', service: 'polybot-weather-outlier-sniper-busan.service' },
  { id: 'live_weather_outlier_sniper_cape_town_auto_v1', service: 'polybot-weather-outlier-sniper-cape-town.service' },
  { id: 'live_weather_outlier_sniper_chengdu_auto_v1', service: 'polybot-weather-outlier-sniper-chengdu.service' },
  { id: 'live_weather_outlier_sniper_chongqing_auto_v1', service: 'polybot-weather-outlier-sniper-chongqing.service' },
  { id: 'live_weather_outlier_sniper_dallas_auto_v1', service: 'polybot-weather-outlier-sniper-dallas.service' },
  { id: 'live_weather_outlier_sniper_guangzhou_auto_v1', service: 'polybot-weather-outlier-sniper-guangzhou.service' },
  { id: 'live_weather_outlier_sniper_helsinki_auto_v1', service: 'polybot-weather-outlier-sniper-helsinki.service' },
  { id: 'live_weather_outlier_sniper_houston_auto_v1', service: 'polybot-weather-outlier-sniper-houston.service' },
  { id: 'live_weather_outlier_sniper_istanbul_auto_v1', service: 'polybot-weather-outlier-sniper-istanbul.service' },
  { id: 'live_weather_outlier_sniper_jakarta_auto_v1', service: 'polybot-weather-outlier-sniper-jakarta.service' },
  { id: 'live_weather_outlier_sniper_jeddah_auto_v1', service: 'polybot-weather-outlier-sniper-jeddah.service' },
  { id: 'live_weather_outlier_sniper_karachi_auto_v1', service: 'polybot-weather-outlier-sniper-karachi.service' },
  { id: 'live_weather_outlier_sniper_kuala_lumpur_auto_v1', service: 'polybot-weather-outlier-sniper-kuala-lumpur.service' },
  { id: 'live_weather_outlier_sniper_los_angeles_auto_v1', service: 'polybot-weather-outlier-sniper-los-angeles.service' },
  { id: 'live_weather_outlier_sniper_madrid_auto_v1', service: 'polybot-weather-outlier-sniper-madrid.service' },
  { id: 'live_weather_outlier_sniper_manila_auto_v1', service: 'polybot-weather-outlier-sniper-manila.service' },
  { id: 'live_weather_outlier_sniper_miami_auto_v1', service: 'polybot-weather-outlier-sniper-miami.service' },
  { id: 'live_weather_outlier_sniper_milan_auto_v1', service: 'polybot-weather-outlier-sniper-milan.service' },
  { id: 'live_weather_outlier_sniper_moscow_auto_v1', service: 'polybot-weather-outlier-sniper-moscow.service' },
  { id: 'live_weather_outlier_sniper_munich_auto_v1', service: 'polybot-weather-outlier-sniper-munich.service' },
  { id: 'live_weather_outlier_sniper_nyc_auto_v1', service: 'polybot-weather-outlier-sniper-nyc.service' },
  { id: 'live_weather_outlier_sniper_panama_city_auto_v1', service: 'polybot-weather-outlier-sniper-panama-city.service' },
  { id: 'live_weather_outlier_sniper_paris_auto_v1', service: 'polybot-weather-outlier-sniper-paris.service' },
  { id: 'live_weather_outlier_sniper_qingdao_auto_v1', service: 'polybot-weather-outlier-sniper-qingdao.service' },
  { id: 'live_weather_outlier_sniper_san_francisco_auto_v1', service: 'polybot-weather-outlier-sniper-san-francisco.service' },
  { id: 'live_weather_outlier_sniper_sao_paulo_auto_v1', service: 'polybot-weather-outlier-sniper-sao-paulo.service' },
  { id: 'live_weather_outlier_sniper_seattle_auto_v1', service: 'polybot-weather-outlier-sniper-seattle.service' },
  { id: 'live_weather_outlier_sniper_shenzhen_auto_v1', service: 'polybot-weather-outlier-sniper-shenzhen.service' },
  { id: 'live_weather_outlier_sniper_singapore_auto_v1', service: 'polybot-weather-outlier-sniper-singapore.service' },
  { id: 'live_weather_outlier_sniper_taipei_auto_v1', service: 'polybot-weather-outlier-sniper-taipei.service' },
  { id: 'live_weather_outlier_sniper_tel_aviv_auto_v1', service: 'polybot-weather-outlier-sniper-tel-aviv.service' },
  { id: 'live_weather_outlier_sniper_toronto_auto_v1', service: 'polybot-weather-outlier-sniper-toronto.service' },
  { id: 'live_weather_outlier_sniper_warsaw_auto_v1', service: 'polybot-weather-outlier-sniper-warsaw.service' },
  { id: 'live_weather_outlier_sniper_wuhan_auto_v1', service: 'polybot-weather-outlier-sniper-wuhan.service' },
];

function mappings() {
  if (process.env.ARB_SNIPER_MAPPINGS_JSON) return JSON.parse(process.env.ARB_SNIPER_MAPPINGS_JSON);
  if (process.env.ARB_SNIPER_STRATEGY_ID || process.env.ARB_SNIPER_SERVICE) {
    return [{ id: process.env.ARB_SNIPER_STRATEGY_ID || 'live_arb_sniper_btc15m_v1', service: process.env.ARB_SNIPER_SERVICE || 'polybot-arb-sniper.service' }];
  }
  return DEFAULT_MAPPINGS;
}

function loadEnv(path) {
  const out = {};
  if (!fs.existsSync(path)) return out;
  for (const line of fs.readFileSync(path, 'utf8').split(/\r?\n/)) {
    if (!line || line.trim().startsWith('#') || !line.includes('=')) continue;
    const i = line.indexOf('=');
    const k = line.slice(0, i).trim();
    let v = line.slice(i + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) v = v.slice(1, -1);
    out[k] = v;
  }
  return out;
}

function systemctl(args, opts = {}) {
  return execFileSync('systemctl', ['--user', ...args], { encoding: 'utf8', stdio: opts.stdio || ['ignore', 'pipe', 'pipe'] });
}

function isActive(service) {
  try {
    systemctl(['is-active', '--quiet', service]);
    return true;
  } catch {
    return false;
  }
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
    for (const mapping of mappings()) {
      const strategyId = mapping.id || mapping.strategy_id;
      const service = mapping.service;
      if (!strategyId || !service) continue;
      const res = await client.query('SELECT status FROM strategies WHERE id=$1', [strategyId]);
      if (res.rowCount === 0) continue;
      const status = String(res.rows[0].status || 'stopped');
      const active = isActive(service);

      if (status === 'running') {
        if (!active) {
          systemctl(['start', service]);
          console.log(`${new Date().toISOString()} started ${service} for ${strategyId}`);
        }
        continue;
      }

      if (status === 'stop_requested' || status === 'stopped') {
        if (active) {
          systemctl(['stop', service]);
          console.log(`${new Date().toISOString()} stopped ${service} for ${strategyId} status=${status}`);
        }
        if (status === 'stop_requested') {
          await client.query("UPDATE strategies SET status='stopped', updated_at=now() WHERE id=$1 AND status='stop_requested'", [strategyId]);
          console.log(`${new Date().toISOString()} marked ${strategyId} stopped`);
        }
      }
    }
  } finally {
    await client.end();
  }
})().catch((err) => {
  console.error(err.stack || err.message || err);
  process.exit(1);
});
