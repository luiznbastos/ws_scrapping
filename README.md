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
- Extracts raw footbal match and event data and persists raw JSON/NDJSON artifacts.
- Implements retries and pacing to respect source limits.
## Orchestration & Pipeline Context
- Cloud execution: this job is triggered by the orchestrator.
- Previous step: infrastructure provisioned by `ws_infrastructure` (S3/ECR/Batch).
- Next step: `ws_preprocessing` consumes raw data and loads staging/bronze.
## Structure
```
ws_scrapping/
├── src/
│   ├── scrappers/
│   ├── driver/
│   ├── utils/
│   └── runner.py
├── Dockerfile
├── Makefile
└── terraform/
```
## Configuration
- Settings: `src/scrappers/settings.py`

### Environment Variables

**Required:**
- `AWS_REGION`: AWS region

**Optional (retrieved from SSM or env):**
- `RUN_ID`: Unique identifier for this run (auto-generated if not provided)
- Source parameters (tournament, date range, etc.) per scraper

**S3 (from SSM when running in Batch):**
- `/${project}/s3/analytics/name`: S3 bucket for raw data (mapped to `S3_BUCKET`)

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

The service runs as an AWS Batch job:

1. Build Docker image:
```
docker build -t ws_scrapping:latest .
```

2. Push to ECR:
```
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin <account-id>.dkr.ecr.us-east-1.amazonaws.com
docker tag ws_scrapping:latest <account-id>.dkr.ecr.us-east-1.amazonaws.com/ws_scrapping:latest
docker push <account-id>.dkr.ecr.us-east-1.amazonaws.com/ws_scrapping:latest
```

3. Submit job via AWS Batch (example):
```
aws batch submit-job \
  --job-name ws-scrapping-$(date +%s) \
  --job-definition ws-analytics-scrapping \
  --job-queue ws-analytics-job-queue
```
## Outputs
- Raw JSON/NDJSON per match/event series to S3 (raw prefix) or local in dev.
## Infrastructure Used
- AWS Batch job definition referencing shared queue and ECR image; S3; SSM; CloudWatch.
## Citation
Soccermatics inspiration: https://soccermatics.readthedocs.io/en/latest/index.html

