import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from collections import defaultdict

LAUNCH_DAY = datetime(2025, 1, 25).date()  

# Setup logging
log_file = "Logs/Export.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Define columns
META_COLS = [
    "customer_id", "imsi", "msisdn", "billing_type", "service_type",
    "call_direction", "call_category", "date"
]

METRICS_OLS = [
    "balance", "charged", "consumed_data", "consumed_onn_calls", "consumed_ofn_calls", "call_duration", "sms_request"
]

def infer_billing(file_path):
    path_str = str(file_path).lower()
    if "prepaid" in path_str:
        return "prepaid"
    elif "postpaid" in path_str:
        return "postpaid"

def infer_service(file_path):
    path_str = str(file_path).lower()
    if "sms" in path_str:
        return "sms"
    if "data" in path_str:
        return "data"
    if "sms" in path_str:
        return "sms"
    if "call" in path_str:
        if "international" in path_str:
            return "internatinal call"
        elif "cug" in path_str:
            return "cug call"
        else:
            return "national call"
    if "data" in path_str:
        return "data"

def infer_direction(path_parts):
    parts = [p.lower() for p in path_parts]
    if any("outgoing" in p for p in parts):
        return "outgoing"
    if any("incoming" in p for p in parts):
    if any("outgoing" in p for p in parts):
        return "outgoing"
    if any("incoming" in p for p in parts):
        return "incoming"
    if any("data" == p for p in parts):
    if any("data" == p for p in parts):
        return "data"

def infer_category(path_parts):
    parts = [p.lower() for p in path_parts]
    if "off_net" in parts:
        return "offnet"
    return "onnet"

def split_json_line(line):
    # Safely extract all JSON objects from a line
    decoder = json.JSONDecoder()
    idx = 0
    records = []
    while idx < len(line):
        try:
            obj, offset = decoder.raw_decode(line[idx:])
            records.append(obj)
            idx += offset
        except json.JSONDecodeError:
            idx += 1
    return records

def extract_date_from_path(file_path):
    for part in reversed(file_path.parts):
        try:
            if len(part) == 10:
                return datetime.strptime(part, "%Y-%m-%d").date()
            elif len(part) == 7:
                return datetime.strptime(part, "%Y-%m").date()
            elif len(part) == 4:
                return datetime.strptime(part, "%Y").date()
        except Exception:
            continue
    return None

def summarize_file(file_path: Path):
    try:
        with file_path.open("r", encoding="utf-8") as f:
            content = f.read()

        all_records = []
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            records = split_json_line(line)
            for rec in records:
                try:
                    rec_msisdn = rec.get("calling_party") or rec.get("sms_sender") or rec.get("msisdn")
                    rec_date = rec.get("data_requested_time") or rec.get("call_start_time") or rec.get("callStartTs") or extract_date_from_path(file_path)
                    rec_balance = rec.get("customer_balance") or rec.get("available_limit")
                    rec_deduction = rec.get("balance_deduct_on_sms") or rec.get("balance_deduct_on_call") or rec.get("deducted_limit")
                    if rec_date is not None:
                        try:
                            rec_date = pd.to_datetime(rec_date).strftime('%Y-%m-%d')
                        except Exception:
                            rec_date = None
                    rec["date"] = rec_date
                    rec["msisdn"] = rec_msisdn
                    rec['balance'] = rec_balance
                    rec['charged'] = rec_deduction
                    all_records.append(rec)
                except Exception as e:
                    logging.error("Skip Record: {e}")

        if not all_records:
            logging.info(f"No valid records in file: {file_path}")
            return None, None

        df = pd.DataFrame(all_records).sort_values("date")
        latest = df.iloc[-1]

        # Enrich metadata from path
        direction = infer_direction(file_path.parts)
        category = infer_category(file_path.parts)
        billing = infer_billing(file_path)
        service = infer_service(file_path)

        tag = f"{billing}_{service}"

        # Assemble summary row
        row = {col: latest.get(col) for col in META_COLS if col in latest}
        row.update({"billing_type": billing, "service_type": service, "direction": direction, "category": category, "date":rec_date})

        for col in METRICS_OLS:
            if col == "consumed_data" and col in df.columns:
                row[col] = df[col].sum()
            elif col == "consumed_onn_calls" and col in df.columns:
                row[col] = df[col].sum()
            elif col == "consumed_ofn_calls" and col in df.columns:
                row[col] = df[col].sum()
            elif col == "sms_request" and col in df.columns:
                row[col] = df[col].sum()
            elif col == "call_duration" and col in df.columns:
                row[col] = df[col].sum() / 60
            else:
                row[col] = 0

        return tag, row

    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None, None

def process_folder(root_dir, output_dir):
    root_dir = Path(root_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    grouped_rows = defaultdict(list)
    last_direction_folder = None
    for folder in root_dir.rglob("*"):
        if not folder.is_dir():
            continue

        direction = infer_direction(folder.parts)
        service = infer_service(folder.parts)
        
        # # Process only outgoing SMS directories
        # if direction != "outgoing" or service != "sms":
        #     continue 
        
        # Process only outgoing & data directories
        if direction not in ("outgoing", "data"):
            continue 
        
        # Process only outgoing SMS directories
        if direction != "outgoing" or service != "sms":
            continue  # Only process relevant folders

        # Only log when entering a new direction folder
        if folder != last_direction_folder:
            logging.info(f"Processing folder: {folder} (direction: {direction})")
            last_direction_folder = folder

        file_date = extract_date_from_path(folder)
        if file_date and file_date < LAUNCH_DAY:
            logging.debug(f"{folder}, pre_launch:{file_date}")
            continue

        for file_path in folder.glob("*.txt"):
            tag, row = summarize_file(file_path)
            if row:
                grouped_rows[tag].append(row)

    for tag, rows in grouped_rows.items():
        df = pd.DataFrame(rows)
        out_file = output_dir / f"{tag}.csv"
        df.to_csv(out_file, index=False)
        logging.info(f"Saved: {out_file} ({len(df)} rows)")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("root_dir", help="Root directory containing all txt files")
    parser.add_argument("--outdir", default="./Files", help="Output folder for CSVs")
    args = parser.parse_args()

    process_folder(args.root_dir, args.outdir)
