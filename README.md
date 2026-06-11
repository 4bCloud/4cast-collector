# 4Cast Collector

Evidence collector for the 4Cast platform. Optimized for massive parallel metadata extraction from AWS.

## Modes

### 1. CLI Mode (Local)
Collects evidence from a single account and saves to a local file.

```bash
python -m collector.main --account-id <id> --role-arn <arn> --external-id <ext-id> --output evidence.json.zst
```

### 2. Worker Mode (SaaS)
Claims `collect` jobs from the durable **Postgres queue (ADR-1)** and publishes live progress to **Redis (ADR-5)**.

```bash
export WORKER_MODE=true
export WORKER_API_KEY=shared-secret
export DATABASE_URL=postgresql://user:pass@localhost:5432/4cast
export REDIS_URL=redis://localhost:6379/0
python -m collector.main
```

## Architecture

This service implements:
- **ARCH-011:** Uses Postgres as the primary and only job queue.
- **ARCH-012:** Coverage Manifest (reporting success/failure per service/region).
- **ARCH-013:** Evidence Artifact Writer (zstd compressed, redacted, S3 upload).
- **ARCH-014:** Stage Split (claims `collect` jobs).
- **ADR-5:** Redis used only for real-time progress and pub/sub.
