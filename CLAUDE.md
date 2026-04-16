# ws_scrapping — WhoScored Scraper

## Intent
Acquire raw match and event data from WhoScored.com. Runs three sequential, incremental steps: **seasons → matches → events**. Respects pacing; supports force-refresh backfills.

## Stack
Python, Selenium (Chromium or Remote driver), SQLAlchemy (Redshift), boto3, Pydantic settings.

## Pipeline Position
- **Previous**: none (pipeline entrypoint, triggered by `ws_orchestrator`)
- **Next (current)**: `ws_preprocessing` reads S3 JSON → Redshift bronze
- **Next (post-migration)**: `ws_dbt_v2` reads S3 JSON directly

## Outputs
- Redshift tables: `seasons`, `season_matches`, `monthly_matches`, `scrape_runs`
- S3: `events.json` per match (consumed downstream)

## AWS
AWS Batch job (Selenium in container); SSM for S3 bucket name + ECR URL; CI/CD via GitHub Actions → ECR push.

## Key Env
`SCRAPPING_TYPE` (DAILY/DATE_RANGE/FULL_RUN), `TOURNAMENT_NAME`, `TOURNAMENT_URL`, `FORCE_REFRESH_*` (cascade downward).

See [README.md](README.md) for full env var list.
