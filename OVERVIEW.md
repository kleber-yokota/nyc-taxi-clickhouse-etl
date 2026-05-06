# NYC Taxi ClickHouse ETL — Overview

> **For the agent**: This is the system map. Read this first to understand the full pipeline.
> For module-specific details, follow links to `[module]/OVERVIEW.md` and `[module]/DETAILS.md`.

---

## What this system does

ETL pipeline for ingesting NYC Taxi & Limousine Commission (TLC) trip record data from public CDN into Garage (S3-compatible object storage) and ClickHouse (OLAP database) for analytical queries and dashboards.

---

## Main flow

```
NYC TLC CDN → extract → data/*.parquet → push → Garage (S3) → ClickHouse (OLAP) → Analytics
```

### Entry point

```bash
uv run python main.py              # backward compat, delegates to etl.Orchestrator
uv run python -m etl               # incremental mode (default)
ETL_MODE=full uv run python -m etl # full mode (reset everything)
```

Runs `extract` (download parquet files) → `push` (upload to Garage + delete local files) → manifest update.

---

## Modules

| Module | Responsibility | Entry Point | Docs |
|---|---|---|---|
| `etl` | Orchestrate extract → push with shared manifest | `python -m etl` | [OVERVIEW](etl/OVERVIEW.md) · [DETAILS](etl/DETAILS.md) |
| `extract` | Download NYC TLC parquet files from CDN | `extract.core.downloader.run()` | [OVERVIEW](extract/OVERVIEW.md) · [DETAILS](extract/DETAILS.md) |
| `push` | Upload parquet files to S3-compatible storage | `push.core.runner.upload_from_env()` | [OVERVIEW](push/OVERVIEW.md) · [DETAILS](push/DETAILS.md) |
| `transform` | Clean, normalize, transform raw data | — | (not implemented) |
| `load` | Load into ClickHouse for OLAP | — | (not implemented) |

---

## Module dependencies

```
main.py → etl.Orchestrator.run()
etl → extract.downloader.downloader.run()
etl → push.core.runner.upload_from_env()
etl → push.core.push_manifest.list_s3_objects  # rebuild manifest
etl → .push_manifest.json  # write (authority)
push → PushedEntry[]       # data for manifest
push → S3 (list_objects)   # manifest rebuild

extract → (external: NYC TLC CDN)
extract → push (reads .push_manifest.json, read-only)
push → (external: Garage/S3 via boto3)
```

`extract` and `push` share the `data/` directory. `extract` reads `.push_manifest.json` (written by orchestrator) to skip downloading files already in S3. `push` never reads from `extract`. The orchestrator is the authority on the manifest: it creates, updates, and saves it, delegating execution to `extract` and `push`.

---

## Cross-module contracts

| Contract | Module A (producer) | Module B (consumer) |
|---|---|---|
| File format | Downloads `.parquet` files | Uploads `*.parquet` files |
| Directory | Writes to `data/<type>/` | Reads from `data/` recursively |
| Naming | `<type>_tripdata_<year>-<month>.parquet` | Matches by glob `*.parquet` |
| State files | `data/.download_state.json` | `data/.push_state.json` |
| Push manifest | Writes `.push_manifest.json` with uploaded file list | Reads manifest to skip already-uploaded files |
| S3 objects (neutral) | — | `push.list_s3_objects()` → `S3Object[key]` |
| Pushed entries (neutral) | — | `push.upload()` → `PushedEntry[]` |
| Orchestrator config | `etl.ETLConfig` | `etl.Orchestrator` |

---

## External dependencies

- `requests` — HTTP client for downloading parquet files (extract)
- `boto3` — AWS SDK for S3/Garage operations (push)
- `minio` — MinIO client (test dependencies)
- `python-dotenv` — Load `.env` for environment variables (main)

### Test dependencies

- `pytest`, `hypothesis`, `mutmut`, `atheris` — testing toolchain
- `testcontainers` — MinIO container for E2E tests
- `responses`, `vcrpy` — HTTP mocking

---

## Configuration

### Environment variables (`.env`)

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `S3_BUCKET` | yes | — | Garage bucket name |
| `S3_ENDPOINT_URL` | no | — | Garage endpoint (e.g. `http://localhost:3600`) |
| `AWS_ACCESS_KEY_ID` | via boto3 | — | S3 credentials |
| `AWS_SECRET_ACCESS_KEY` | via boto3 | — | S3 credentials |

### Extract configuration

| Parameter | Default | Purpose |
|---|---|---|
| `types` | all (fhv, fhvhv, green, yellow) | Data types to download |
| `from_year` | 2009 | Start year |
| `to_year` | current year | End year |
| `mode` | `incremental` | `full` resets state, `incremental` preserves |

---

## Data directory structure

```
data/
├── .download_state.json    # Extract state (URL → checksum)
├── .push_manifest.json     # Push manifest (path → {s3_key, checksum})
├── .push_manifest.json     # Push manifest (uploaded files list, shared with extract)
├── errors/
│   └── download_errors.log  # Extract error log (JSON lines)
├── known_missing.txt        # Extract 404 tracker
├── fhv/
│   └── fhv_tripdata_2024-01.parquet
├── fhvhv/
│   └── fhvhv_tripdata_2024-01.parquet
├── green/
│   └── green_tripdata_2024-01.parquet
└── yellow/
    └── yellow_tripdata_2024-01.parquet
```

---

## What is NOT in this system

- Web UI → see `garage-ui` (separate container)
- ClickHouse queries → see ClickHouse client tools
- Data transformation logic → `transform/` (not implemented)
- Dashboard → external tool (Grafana, etc.)

---

## Files to read if you need more context

| File | Why read it |
|---|---|
| `extract/OVERVIEW.md` | Extract module flow and structure |
| `extract/DETAILS.md` | Extract function signatures, edge cases |
| `push/OVERVIEW.md` | Push module flow and structure |
| `push/DETAILS.md` | Push function signatures, edge cases |
| `New.md` | Architectural guardrails and quality rules |
| `TESTING_GUIDE.md` | Testing instructions (pytest, hypothesis, mutmut, atheris) |
| `AGENTS.md` | Documentation protocol and Python tooling rules |
