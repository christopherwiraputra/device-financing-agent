import os
import time
import subprocess
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
sys.stdout.reconfigure(encoding='utf-8')
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────
CSV_PATH    = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\data\device_financing_messy.csv"
DBT_DIR     = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\dbt_project\device_financing"
DBT_DB_PATH = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\dbt_project\device_financing.duckdb"

GMAIL_SENDER    = os.getenv("GMAIL_SENDER")
GMAIL_PASSWORD  = os.getenv("GMAIL_APP_PASSWORD")
GMAIL_RECIPIENT = os.getenv("GMAIL_RECIPIENT")

POLL_INTERVAL_SECONDS = 10
# ──────────────────────────────────────────────────────────────


def get_file_modified_time(path):
    return os.path.getmtime(path)


def run_dbt():
    """Run dbt seed + run + test. Returns True if all pass."""
    print("\n[Pipeline] Running dbt pipeline...")

    for command in ["dbt seed", "dbt run", "dbt test"]:
        print(f"[Pipeline] Running: {command}")
        result = subprocess.run(
            command,
            shell=True,
            cwd=DBT_DIR,
            capture_output=True,
            text=True
        )
        print(result.stdout[-300:])

        if result.returncode != 0:
            print(f"[Pipeline] {command} failed.")
            print(result.stderr[-300:])
            return False

    print("[Pipeline] dbt pipeline completed successfully.")
    return True


def score_applications():
    """Call batch_score.py as a subprocess."""
    print("[Pipeline] Scoring new applications...")

    scorer = r"C:\Users\HP\Desktop\USC\Projects\Device Agent\agent\batch_score.py"
    result = subprocess.run(
        ["py", scorer],
        capture_output=True,
        encoding='utf-8',
        errors='replace'
    )

    if result.returncode != 0:
        print(f"[Pipeline] Scoring failed: {result.stderr[-300:]}")
        return []

    if not result.stdout.strip():
        print("[Pipeline] No output from scoring script.")
        return []

    return [result.stdout]


def build_email_body(reports):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    header = f"""FRAGILE DEVICE FINANCING — AUTOMATED UNDERWRITING REPORT
Generated: {timestamp}
{'='*65}
"""
    return header + "\n".join(reports)


def send_email(subject, body):
    """Send report email via Gmail SMTP."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = GMAIL_RECIPIENT
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_SENDER, GMAIL_RECIPIENT, msg.as_string())
        print(f"[Pipeline] Email sent to {GMAIL_RECIPIENT}")
    except Exception as e:
        print(f"[Pipeline] Email failed: {e}")



def main():
    print("="*65)
    print("  FRAGILE DEVICE FINANCING — PIPELINE ORCHESTRATOR")
    print("="*65)
    print(f"[Pipeline] Watching: {CSV_PATH}")
    print(f"[Pipeline] Poll interval: {POLL_INTERVAL_SECONDS} seconds")
    print("[Pipeline] Waiting for CSV changes... (Ctrl+C to stop)\n")

    last_modified = get_file_modified_time(CSV_PATH)

    while True:
        time.sleep(POLL_INTERVAL_SECONDS)
        current_modified = get_file_modified_time(CSV_PATH)

        if current_modified != last_modified:
            last_modified = current_modified
            print(f"\n[Pipeline] Change detected at {time.strftime('%H:%M:%S')}")

            # step 1: run dbt
            dbt_success = run_dbt()

            if not dbt_success:
                subject = f"Fragile Underwriting Report — New Applications Processed"
                body = build_email_body(reports)
                send_email(subject, body)
                continue

            # step 2: score applications via subprocess
            reports = score_applications()

            if not reports:
                print("[Pipeline] No reports generated.")
                continue

            # step 3: send email
            subject = f"Fragile Underwriting Report — New Applications Processed"
            body = f"FRAGILE DEVICE FINANCING — AUTOMATED UNDERWRITING REPORT\nGenerated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'='*65}\n\n" + "\n".join(reports)
            send_email(subject, body)

        else:
            print(f"[Pipeline] No changes detected. ({time.strftime('%H:%M:%S')})")


if __name__ == "__main__":
    main()
