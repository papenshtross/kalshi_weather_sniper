# infra

Local Postgres (TimescaleDB) + Grafana for Nautilus persistence and dashboards.

```bash
cd infra
docker compose up -d
# Grafana: [REDACTED_SECRET_f1de9e489ba8]  (admin / polybot)
# Postgres: postgresql://polybot:polybot@localhost:5432/polybot
```

Set `NAUTILUS_DB_URL` in your `.env` to the Postgres URL so the live runner
writes account/order/fill events there. The Grafana provisioning directory is
a placeholder — drop dashboard JSON files into `grafana/provisioning/dashboards/`
to auto-load them.
