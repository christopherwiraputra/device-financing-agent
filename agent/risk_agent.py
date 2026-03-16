import os
import json
import requests
import duckdb
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
DBT_DB_PATH = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\dbt_project\device_financing.duckdb"


def get_segment_benchmarks(con, device_type, credit_tier, lease_term_months):
    """Pull comparable segment benchmarks from the mart."""
    query = """
        select
            total_applications,
            total_defaults,
            default_rate,
            avg_credit_score,
            avg_dti_ratio,
            avg_monthly_payment,
            avg_device_msrp,
            avg_monthly_income,
            avg_composite_risk_score,
            avg_credit_utilization,
            avg_payment_to_income_ratio
        from mart_device_risk
        where credit_tier = ?
          and device_type = ?
          and lease_term_months = ?
        limit 1
    """
    result = con.execute(query, [credit_tier, device_type, lease_term_months]).fetchdf()

    if result.empty:
        # fallback: just match credit tier
        query_fallback = """
            select
                sum(total_applications)             as total_applications,
                sum(total_defaults)                 as total_defaults,
                round(avg(default_rate), 3)         as default_rate,
                round(avg(avg_credit_score), 0)     as avg_credit_score,
                round(avg(avg_dti_ratio), 3)        as avg_dti_ratio,
                round(avg(avg_monthly_payment), 2)  as avg_monthly_payment,
                round(avg(avg_device_msrp), 2)      as avg_device_msrp,
                round(avg(avg_monthly_income), 2)   as avg_monthly_income,
                round(avg(avg_composite_risk_score), 2) as avg_composite_risk_score,
                round(avg(avg_credit_utilization), 3)   as avg_credit_utilization,
                round(avg(avg_payment_to_income_ratio), 3) as avg_payment_to_income_ratio
            from mart_device_risk
            where credit_tier = ?
        """
        result = con.execute(query_fallback, [credit_tier]).fetchdf()

    if result.empty:
        return None

    return result.iloc[0].to_dict()


def call_claude(application, benchmarks):
    """Send application + benchmarks to Claude via OpenRouter."""

    benchmarks_text = "No benchmark data available." if not benchmarks else f"""
- Total comparable applications: {benchmarks.get('total_applications', 'N/A')}
- Historical default rate: {float(benchmarks.get('default_rate', 0)):.1%}
- Avg credit score in segment: {benchmarks.get('avg_credit_score', 'N/A')}
- Avg DTI ratio in segment: {benchmarks.get('avg_dti_ratio', 'N/A')}
- Avg monthly payment in segment: ${benchmarks.get('avg_monthly_payment', 'N/A')}
- Avg monthly income in segment: ${benchmarks.get('avg_monthly_income', 'N/A')}
- Avg composite risk score: {benchmarks.get('avg_composite_risk_score', 'N/A')}
- Avg credit utilization: {float(benchmarks.get('avg_credit_utilization', 0)):.1%}
    """.strip()

    prompt = f"""You are an underwriting analyst at Fragile, a hardware financing company. 
Your job is to evaluate device financing applications and provide clear, structured recommendations.

APPLICATION DETAILS:
- Application ID: {application['application_id']}
- Device: {application['device_type']}
- Device MSRP: ${application['device_msrp']}
- Lease Term: {application['lease_term_months']} months
- Monthly Payment: ${application['monthly_payment']}
- Credit Tier: {application['credit_tier']}
- Credit Score: {application['credit_score']} {'(imputed)' if application['credit_score_imputed_flag'] else ''}
- Monthly Income: ${application['monthly_income']} {'(imputed)' if application['monthly_income_imputed_flag'] else ''}
- Employment Type: {application['employment_type']}
- DTI Ratio: {application['dti_ratio']} {'(imputed)' if application['dti_ratio_imputed_flag'] else ''}
- Credit Utilization: {application['credit_utilization']} {'(imputed)' if application['credit_utilization_imputed_flag'] else ''}
- Prior Leases Completed: {application['prior_leases_completed']}
- Prior Defaults: {application['prior_defaults']}
- Composite Risk Score: {application['composite_risk_score']} (scale 0-3, higher = riskier)
- Risk Tier: {application['risk_tier']}
- Region: {application['region']}

SEGMENT BENCHMARKS (similar applications in our portfolio):
{benchmarks_text}

Based on this application and the segment benchmarks, provide a structured underwriting recommendation.
Respond in this exact JSON format:
{{
    "recommendation": "APPROVED" | "CONDITIONAL APPROVAL" | "DECLINED",
    "confidence": "High" | "Medium" | "Low",
    "risk_summary": "2-3 sentence summary of the key risk factors",
    "positive_factors": ["factor 1", "factor 2"],
    "risk_factors": ["factor 1", "factor 2"],
    "conditions": ["condition 1", "condition 2"],
    "suggested_action": "One clear sentence on what to do"
}}

If recommendation is APPROVED, conditions can be an empty list.
If recommendation is DECLINED, conditions should explain what would need to change for reconsideration.
Respond with JSON only, no other text."""

    response = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": "anthropic/claude-sonnet-4-5",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 1000,
            "temperature": 0.1
        }
    )

    response.raise_for_status()
    content = response.json()["choices"][0]["message"]["content"]

    # strip markdown fences if present
    content = content.strip()
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
    content = content.strip()

    return json.loads(content)


def format_report(application, benchmarks, recommendation):
    """Format the final underwriting report."""

    rec = recommendation["recommendation"]
    rec_color = {
        "APPROVED": "✅",
        "CONDITIONAL APPROVAL": "⚠️",
        "DECLINED": "❌"
    }.get(rec, "")

    conditions_text = ""
    if recommendation.get("conditions"):
        conditions_text = "\n  CONDITIONS:\n" + \
            "\n".join(f"    • {c}" for c in recommendation["conditions"])

    report = f"""
{'='*65}
  FRAGILE DEVICE FINANCING — UNDERWRITING REPORT
{'='*65}
  Application ID : {application['application_id']}
  Device         : {application['device_type']} (${application['device_msrp']} MSRP)
  Lease Term     : {application['lease_term_months']} months @ ${float(application['monthly_payment']):.2f}/mo
  Applicant      : {application['credit_tier'].upper()} credit | Score: {application['credit_score_imputed']} {'*' if application['credit_score_imputed_flag'] else ''}
  Employment     : {application['employment_type']} | Income: ${application['monthly_income_imputed']}/mo {'*' if application['monthly_income_imputed_flag'] else ''}
  DTI Ratio      : {float(application['dti_ratio_imputed']):.3f} {'*' if application['dti_ratio_imputed_flag'] else ''} | Risk Score: {application['composite_risk_score']}/3.0

  SEGMENT BENCHMARKS:
  Comparable apps  : {int(benchmarks.get('total_applications', 0)) if benchmarks else 'N/A'}
  Segment default rate: {f"{float(benchmarks.get('default_rate', 0)):.1%}" if benchmarks else 'N/A'}
  Avg credit score : {benchmarks.get('avg_credit_score', 'N/A') if benchmarks else 'N/A'}

  DECISION: {rec_color} {rec} (Confidence: {recommendation['confidence']})
{'─'*65}
  RISK SUMMARY:
  {recommendation['risk_summary']}

  POSITIVE FACTORS:
{chr(10).join(f"    • {f}" for f in recommendation.get('positive_factors', []))}

  RISK FACTORS:
{chr(10).join(f"    • {f}" for f in recommendation.get('risk_factors', []))}
{conditions_text}
  SUGGESTED ACTION:
  {recommendation['suggested_action']}

  * = imputed value (original was missing)
{'='*65}
"""
    return report


def score_application(con, application_id):
    """Score a single application by ID."""

    query = """
        select *
        from int_device_financing_enriched
        where application_id = ?
    """
    result = con.execute(query, [application_id]).fetchdf()

    if result.empty:
        print(f"Application {application_id} not found.")
        return None

    application = result.iloc[0].to_dict()

    benchmarks = get_segment_benchmarks(
        con,
        application['device_type'],
        application['credit_tier'],
        application['lease_term_months']
    )

    recommendation = call_claude(application, benchmarks)
    report = format_report(application, benchmarks, recommendation)

    return report


def main():
    print("\n" + "="*65)
    print("  FRAGILE DEVICE FINANCING — UNDERWRITING AGENT")
    print("="*65)

    con = duckdb.connect(DBT_DB_PATH, read_only=True)

    print("\nEnter an application ID to score (e.g. APP-00001)")
    print("Or press Enter to score a random application")
    app_id = input("\nApplication ID: ").strip()

    if not app_id:
        result = con.execute(
            "select application_id from int_device_financing_enriched order by random() limit 1"
        ).fetchone()
        app_id = result[0]
        print(f"Randomly selected: {app_id}")

    print(f"\nScoring {app_id}...")
    report = score_application(con, app_id)

    if report:
        print(report)
    con.close()


if __name__ == "__main__":
    main()

con = duckdb.connect(DBT_DB_PATH, read_only=True)
con.execute("USE main") 
