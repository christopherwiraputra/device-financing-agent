# Device Financing Underwriting Agent

An event-driven analytics pipeline that automatically scores device financing 
applications and delivers structured underwriting recommendations via email.

## What It Does

When a new financing application is added to the source CSV, the pipeline automatically:

1. **Detects the file change** — Python watches the CSV every 10 seconds
2. **Runs the full dbt pipeline** — seed → run → 12 data quality tests
3. **Scores the new application** — queries segment benchmarks from the mart, calls Claude via API
4. **Emails a structured underwriting report** — delivered to the underwriter's inbox within ~60 seconds

No manual intervention required. Add a row, get an email.

## Architecture
```
CSV updated (new application row added)
        ↓
Python pipeline detects file change (polls every 10 seconds)
        ↓
dbt seed → dbt run → dbt test (12 data quality tests must pass)
        ↓
Agent queries mart for segment benchmarks
        ↓
Claude reasons about risk profile vs benchmarks
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
seeds/device_financing_messy.csv                   ← raw synthetic data (2,028 rows, 19 features)
        ↓
models/staging/stg_device_financing                ← standardize, nullify invalid values, quality flags
        ↓
models/intermediate/int_device_financing_enriched  ← feature engineering, imputation, risk scoring
        ↓
models/marts/mart_device_risk                      ← aggregated segment benchmarks
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
- `composite_risk_score` — weighted risk score (0–2.4) from credit (40%), DTI (30%), prior defaults (20%), employment (10%)
- `risk_tier` — low_risk / moderate_risk / elevated_risk / high_risk
- `payment_to_income_ratio` — affordability signal
- `high_dti_flag` — DTI > 0.5 threshold flag
- `has_prior_default` — boolean default history flag
- `is_experienced_lessee` — prior lease completion signal
- `device_value_tier` — premium / mid_range / budget
- Segment-average imputation for missing credit score, DTI, income, and utilization

### dbt Tests (12 total)
- `unique` and `not_null` on application_id
- `accepted_values` on device_type, credit_tier, lease_term_months, employment_type
- `not_null` on mart aggregation columns
- Custom singular test: `assert_valid_risk_scores` — ensures composite score stays within expected bounds

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

This JSON is then formatted into a human-readable report and delivered via email.

## Sample Output
```
=================================================================
  FRAGILE DEVICE FINANCING — UNDERWRITING REPORT
=================================================================
  Application ID : APP-01841
  Device         : iPhone 13 ($599 MSRP)
  Lease Term     : 36 months @ $19.77/mo
  Applicant      : POOR credit | Score: 476
  Employment     : unemployed | Income: $730/mo
  DTI Ratio      : 0.521 * | Risk Score: 2.4/3.0

  SEGMENT BENCHMARKS:
  Comparable apps     : 22
  Segment default rate: 40.9%
  Avg credit score    : 452

  DECISION: DECLINED (Confidence: High)
─────────────────────────────────────────────────────────────────
  RISK SUMMARY:
  Unacceptable risk — unemployed with very low income ($730/mo),
  two prior defaults, and composite risk score of 2.4 significantly
  exceeds the segment average of 1.55.

  POSITIVE FACTORS:
    • Monthly payment of $19.77 represents only 2.7% of stated income
    • One prior lease successfully completed
    • Credit utilization at 60% is below segment average of 73.5%

  RISK FACTORS:
    • Unemployed with no verifiable income source
    • Two prior defaults indicating pattern of non-payment
    • Credit score of 476 is extremely low
    • Segment default rate of 40.9% confirms high-risk pool

  CONDITIONS:
    • Obtain stable employment with income above $2,000/mo
    • Provide co-signer with credit score above 650
    • Increase down payment to 30% ($180)

  SUGGESTED ACTION:
  Decline — advise applicant to reapply once employed with
  stable income and a co-signer.

  * = imputed value (original was missing)
=================================================================
```

## Setup

1. Install dependencies:
```bash
pip install dbt-duckdb duckdb python-dotenv requests pandas
```

2. Create a `.env` file in the project root (see `.env.example` for reference):
```
OPENROUTER_API_KEY=your_key_here
GMAIL_SENDER=your_gmail@gmail.com
GMAIL_APP_PASSWORD=your_app_password
GMAIL_RECIPIENT=recipient@gmail.com
CSV_PATH=path/to/seeds/device_financing_messy.csv
DBT_DIR=path/to/dbt_project/device_financing
DBT_DB_PATH=path/to/device_financing.duckdb
SCORER_PATH=path/to/agent/batch_score.py
TRACKER_PATH=path/to/data/last_processed.txt
PROJECT_ROOT=path/to/project/root
```

3. Run the dbt pipeline:
```bash
cd dbt_project/device_financing
dbt seed
dbt run
dbt test
```

4. Start the pipeline orchestrator:
```bash
python agent/pipeline.py
```

5. Add a new row to the source CSV — the pipeline will automatically detect 
the change, run dbt, score the application, and email the report.

## Production Mapping

This project mirrors production analytics engineering workflows at hardware financing companies:

| This Project | Production Equivalent |
|---|---|
| CSV file change | New application row landing in Snowflake |
| Python file watcher | Dagster sensor detecting new rows |
| DuckDB | Snowflake |
| Local dbt run | Dagster-orchestrated dbt job |
| last_processed.txt tracker | Watermark column in database |
| Gmail SMTP | Slack alert or internal dashboard |