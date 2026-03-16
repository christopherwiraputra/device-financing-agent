# Device Financing Underwriting Agent

An event-driven analytics pipeline that automatically scores device financing 
applications and delivers structured underwriting recommendations via email.

## Architecture
```
CSV updated (new application)
        ↓
Python pipeline detects file change
        ↓
dbt seed → run → test (12 data quality tests)
        ↓
Claude (via OpenRouter) reasons about risk
        ↓
Structured underwriting report emailed automatically
```

## Tech Stack

- **dbt + DuckDB** — three-layer data transformation pipeline
- **Python** — pipeline orchestrator and agent
- **Claude (Anthropic)** — LLM reasoning layer via OpenRouter
- **Gmail SMTP** — automated report delivery

## dbt Pipeline
```
seeds/device_financing_messy.csv     ← raw synthetic data (2,028 rows, 19 features)
        ↓
models/staging/stg_device_financing  ← standardize, nullify invalid values, quality flags
        ↓
models/intermediate/int_device_financing_enriched  ← feature engineering, imputation, risk scoring
        ↓
models/marts/mart_device_risk        ← aggregated segment benchmarks
```

### Data Quality
The raw dataset intentionally contains real-world messiness:
- Inconsistent casing (`iPhone 14` / `iphone 14` / `IPHONE 14`)
- Invalid values (`credit_score: -999`, `lease_term: 999`)
- Missing values (`N/A`, empty cells, nulls)
- Duplicate rows (~2%)
- Future-dated application entries

The staging layer handles all of this with nullify-and-flag logic — bad values 
are nulled out and companion quality flag columns record *why* they were nulled.

### Feature Engineering (Intermediate Layer)
- `composite_risk_score` — weighted risk score (0-3) from credit, DTI, defaults, employment
- `risk_tier` — low_risk / moderate_risk / elevated_risk / high_risk
- `payment_to_income_ratio` — affordability signal
- `high_dti_flag` — DTI > 0.5 threshold flag
- `has_prior_default` — boolean default history flag
- `is_experienced_lessee` — prior lease completion signal
- `device_value_tier` — premium / mid_range / budget
- Segment-average imputation for missing credit score, DTI, income, utilization

### dbt Tests (12 total)
- `unique` and `not_null` on application_id
- `accepted_values` on device_type, credit_tier, lease_term_months, employment_type
- `not_null` on mart aggregation columns
- Custom singular test: `assert_valid_risk_scores` — ensures composite score stays within 0-3

## Underwriting Agent

The agent pulls segment benchmarks from the mart and passes them alongside 
application details to Claude, which returns a structured JSON recommendation:
```json
{
  "recommendation": "APPROVED | CONDITIONAL APPROVAL | DECLINED",
  "confidence": "High | Medium | Low",
  "risk_summary": "...",
  "positive_factors": ["..."],
  "risk_factors": ["..."],
  "conditions": ["..."],
  "suggested_action": "..."
}
```

## Setup

1. Install dependencies:
```bash
pip install dbt-duckdb duckdb python-dotenv requests
```

2. Create `.env` file:
```
OPENROUTER_API_KEY=your_key_here
GMAIL_SENDER=your_gmail@gmail.com
GMAIL_APP_PASSWORD=your_app_password
GMAIL_RECIPIENT=your_gmail@gmail.com
```

3. Run dbt pipeline:
```bash
cd dbt_project/device_financing
dbt seed
dbt run
dbt test
```

4. Start the pipeline orchestrator:
```bash
python pipeline.py
```

5. Add a new row to `data/device_financing_messy.csv` — the pipeline will 
automatically detect the change, run dbt, score the application, and email the report.

## Project Context

Built as a portfolio project demonstrating the analytics engineering stack 
used at hardware financing companies. The architecture mirrors production 
systems where:
- CSV/file changes → database sensor (Dagster)
- DuckDB → Snowflake
- Local orchestration → Dagster pipeline
- Synthetic device data → real device financing applications
