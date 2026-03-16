import sys
sys.stdout.reconfigure(encoding='utf-8')
import duckdb
import os
import time

sys.path.append(r"C:\Users\HP\Desktop\USC\Projects\Device Agent")

from dotenv import load_dotenv
load_dotenv()

from agent.risk_agent import get_segment_benchmarks, call_claude, format_report

DBT_DB_PATH  = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\dbt_project\device_financing.duckdb"
TRACKER_PATH = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\data\last_processed.txt"


def get_last_processed():
    """Read the last processed application ID from tracker file."""
    if not os.path.exists(TRACKER_PATH):
        return None
    with open(TRACKER_PATH, 'r') as f:
        content = f.read().strip()
        return content if content else None


def save_last_processed(application_id):
    """Save the last processed application ID to tracker file."""
    with open(TRACKER_PATH, 'w') as f:
        f.write(application_id)


def main():
    time.sleep(5)

    last_processed = get_last_processed()
    print(f"Last processed ID: {last_processed}")

    con = duckdb.connect(DBT_DB_PATH, read_only=True)

    if last_processed:
        df = con.execute("""
            select *
            from int_device_financing_enriched
            where application_id > ?
            order by application_id asc
        """, [last_processed]).fetchdf()
    else:
        # first run — get the max ID and save it, score nothing
        max_id = con.execute("""
            select max(application_id)
            from int_device_financing_enriched
        """).fetchone()[0]
        con.close()
        save_last_processed(max_id)
        print(f"First run — saved checkpoint at {max_id}. Will score new rows next time.")
        return

    con.close()

    if df.empty:
        print("No new applications to score.")
        return

    print(f"Scoring {len(df)} new application(s)...")

    for _, row in df.iterrows():
        application = row.to_dict()

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

    last_id = df['application_id'].iloc[-1]
    save_last_processed(last_id)


if __name__ == "__main__":
    main()