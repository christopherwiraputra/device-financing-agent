import sys
sys.stdout.reconfigure(encoding='utf-8')
import duckdb
import os
import time



from dotenv import load_dotenv
load_dotenv()
sys.path.append(os.getenv("PROJECT_ROOT"))
from agent.risk_agent import get_segment_benchmarks, call_claude, format_report

DBT_DB_PATH  = os.getenv("DBT_DB_PATH")
TRACKER_PATH = os.getenv("TRACKER_PATH")


def get_last_row_count():
    """Read the last known row count from tracker file."""
    if not os.path.exists(TRACKER_PATH):
        return None
    with open(TRACKER_PATH, 'r') as f:
        content = f.read().strip()
        return int(content) if content else None


def save_row_count(count):
    """Save the current row count to tracker file."""
    with open(TRACKER_PATH, 'w') as f:
        f.write(str(count))


def main():
    time.sleep(5)

    last_count = get_last_row_count()

    con = duckdb.connect(DBT_DB_PATH, read_only=True)

    total_rows = con.execute("""
        select count(*) from int_device_financing_enriched
    """).fetchone()[0]

    print(f"Last known row count: {last_count} | Current row count: {total_rows}")

    if last_count is None:
        # First run — save current count as checkpoint, score nothing
        con.close()
        save_row_count(total_rows)
        print(f"First run — saved checkpoint at {total_rows} rows. Will score new rows next time.")
        return

    if total_rows <= last_count:
        con.close()
        print("No new rows detected.")
        return

    # Fetch only the single newest row (last by application_id)
    df = con.execute("""
        select *
        from int_device_financing_enriched
        order by application_id desc
        limit 1
    """).fetchdf()

    con.close()

    if df.empty:
        print("Could not retrieve the new row.")
        return

    application = df.iloc[0].to_dict()
    print(f"Scoring newest application: {application['application_id']}...")

    con2 = duckdb.connect(DBT_DB_PATH, read_only=True)
    benchmarks = get_segment_benchmarks(
        con2,
        application['device_type'],
        application['credit_tier'],
        application['lease_term_months']
    )
    con2.close()

    recommendation = call_claude(application, benchmarks)
    report = format_report(application, benchmarks, recommendation)
    print(report)

    save_row_count(total_rows)


if __name__ == "__main__":
    main()