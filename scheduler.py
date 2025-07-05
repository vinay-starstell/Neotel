from apscheduler.schedulers.blocking import BlockingScheduler
from telecom_export import collect_and_group, export_to_csv, load_config
from dashboard import load_all_inputs, build_dashboard
from pathlib import Path
from datetime import datetime
import pytz

def scheduled_task():
    print(f"[{datetime.now()}] Running scheduled data pipeline...")

    # STEP 1: Export updated raw .txt files to Excel
    root_dir = Path("./CDR")
    out_dir = Path("./Files")
    out_dir.mkdir(parents=True, exist_ok=True)

    config = load_config("config.yaml")
    grouped = collect_and_group(root_dir, config.get("include_categories", []))
    export_to_csv(grouped, out_dir)
    print("[✓] Telecom data exported to CSV's.")

    # STEP 2: Generate dashboard
    dfs = load_all_inputs(out_dir)
    output_path = out_dir / "Dashboard01.xlsx"
    build_dashboard(dfs, output_path)
    print(f"[✓] Dashboard written to: {output_path}")

scheduler = BlockingScheduler()
scheduler.add_job(scheduled_task, 'cron', hour=0, minute=0, timezone=pytz.timezone("Asia/Kolkata"))

print("Scheduler started. Daily pipeline will run at 12:00 AM IST.")
scheduler.start()
