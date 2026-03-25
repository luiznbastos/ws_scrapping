# WS Analytics — Scraping Service
## Project Overview (Shared)
- Business objective: deliver actionable soccer performance insights from historical and near‑real‑time data, inspired by Soccermatics methods (https://soccermatics.readthedocs.io/en/latest/index.html).
- Pipeline (end‑to‑end):
  - ws_scrapping: acquire raw match/event data from web sources/APIs
  - ws_preprocessing: validate, normalize, and load to staging (S3→Parquet→Redshift when applicable)
  - ws_dbt: transform into bronze/silver/gold models for analytics (team/player/match)
  - ws_orchestrator: schedule and monitor flows (S3 state when applicable)
  - ws_streamlit: visualize KPIs and match insights
  - ws_infrastructure: IaC for compute, storage, security, CI/CD
- Data stores: AWS S3 and Redshift are primary; not all steps use both.
- Future (planned next year): add an xG/xT training job; extend the pipeline/dbt (Python models or external tasks) to load trained parameters, infer and persist xG/xT per event, and compute aggregates, using dbt tags to separate standard vs inference runs.
## Producer–Consumer Pattern
- Consumer of shared infrastructure produced by `ws_infrastructure`.
- Consumes from SSM:
  - ECR image URL: `/${project}/ecr/${job_name}/url`
  - Batch job queue ARN: `/${project}/batch/job-queue-arn`
  - Analytics bucket name: `/${project}/s3/analytics/name`
- Produces back to SSM:
  - Batch job definition ARN: `/${project}/batch/jobs/${job_name}/arn` (for orchestration)
## What This Service Does
- Extracts raw football match and event data from WhoScored and persists raw JSON artifacts to S3 and metadata to Redshift.
- Runs three sequential steps: **seasons** → **matches** → **events**.
- Each step is **incremental by default**: it detects what already exists in the database/S3 and only processes new data.
- Supports **force refresh** via environment variables to re-scrape specific steps (with downward cascade).
- Implements pacing (sleeps between page loads) to respect source limits.
## Orchestration & Pipeline Context
- Cloud execution: this job is triggered by the orchestrator.
- Previous step: infrastructure provisioned by `ws_infrastructure` (S3/ECR/Batch).
- Next step: `ws_preprocessing` consumes raw data and loads staging/bronze.
## Pipeline Steps

1. **Seasons** (`ScrapeSeasons`): Navigates the tournament page, extracts available season IDs from the HTML dropdown. Saves season metadata (ID, URL, S3 prefix) to the `seasons` table. Skips if seasons already exist unless force-refreshed.

2. **Matches** (`ScrapeMatches`): For each season, navigates the fixtures page and clicks through months to capture network responses with match data. Saves per-month match details to `monthly_matches` and match routing info to `season_matches`. Detects which months are already scraped and only processes new ones unless force-refreshed.

3. **Events** (`ScrapeEvents`): For each match, navigates the live match page and extracts detailed event data (passes, shots, formations, etc.) from embedded JSON. Saves `events.json` to S3 and logs the run to `scrape_runs`. Skips matches that haven't happened yet or already have data in S3 unless force-refreshed.

## Structure
```
ws_scrapping/
├── src/
│   ├── runner.py              # Entry point — runs seasons → matches → events
│   └── scrappers/
│       ├── settings.py        # Pydantic settings (env vars + SSM)
│       ├── task.py            # ScrapeSeasons, ScrapeMatches, ScrapeEvents
│       ├── driver/
│       │   └── network_driver.py  # Selenium/Chrome WebDriver abstraction
│       └── utils/
│           ├── database.py    # Redshift client (SQLAlchemy)
│           └── aws.py         # S3 helpers
├── Dockerfile
├── Makefile
└── terraform/
```
## Configuration
- Settings: `src/scrappers/settings.py`

### Environment Variables

**Required:**
- `AWS_REGION`: AWS region

**Runner:**
- `RUN_ID`: Unique identifier for this run (auto-generated if not provided)
- `SCRAPPING_TYPE`: `DAILY`, `DATE_RANGE`, or `FULL_RUN`
- `DRIVER_TYPE`: `REMOTE` or `CHROMIUM` (default `CHROMIUM`)
- `TOURNAMENT_NAME`: League identifier (e.g. `laliga`)
- `TOURNAMENT_URL`: WhoScored tournament URL
- `SEASON`: Restrict to a single season ID (optional)
- `MATCH`: Restrict to a single match ID (optional)
- `START_DATE` / `END_DATE`: Date range filter for events (format `YYYY-MM-DD`)

**Force Refresh (backfill):**
- `FORCE_REFRESH_SEASONS`: `true` to re-scrape all seasons (cascades to matches and events)
- `FORCE_REFRESH_MATCHES`: `true` to re-scrape all matches (cascades to events)
- `FORCE_REFRESH_EVENTS`: `true` to re-scrape events even if they already exist in S3

Flags cascade downward: enabling seasons implies matches and events; enabling matches implies events.

**S3 (from SSM when running in Batch):**
- `/${project}/s3/analytics/name`: S3 bucket for raw data

## Usage

### Local Development

1. Set up environment variables:
```
export AWS_REGION=us-east-1
# optionally configure source-specific vars
```

2. Install dependencies:
```
pip install -r requirements.txt
```

3. Run the service:
```
python src/runner.py
```

### AWS Batch Deployment

The service runs as an AWS Batch job. Deployment is handled by GitHub Actions on push to `main`:

1. The `.github/workflows/deploy.yml` workflow builds a `linux/amd64` Docker image and pushes it to ECR.
2. The Makefile contains the build/push targets (`make build`, `make push`, `make deploy`).
3. The orchestrator (`ws_orchestrator`) submits Batch jobs with the appropriate environment variables.
## Outputs
- `seasons` table in Redshift: season IDs, URLs, S3 prefixes.
- `season_matches` table: match IDs, URLs, S3 paths, linked to seasons.
- `monthly_matches` table: detailed match metadata per month (teams, scores, times).
- `scrape_runs` table: audit log of which matches were scraped per run.
- `events.json` files in S3: detailed match event data (passes, shots, formations).
## Infrastructure Used
- AWS Batch job definition referencing shared queue and ECR image; S3; SSM; CloudWatch.
## Citation
Soccermatics inspiration: https://soccermatics.readthedocs.io/en/latest/index.html

