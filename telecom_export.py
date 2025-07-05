import json, yaml, sys, logging
import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime


LAUNCH_DAY = datetime(2025, 1, 25).date()           # LaunchDay for NeoTel
TRACKER_FILE = "last_ingested.yaml"                 # To get last refresh date for CDR Logs
SKIPPED_LOGS = Path("Logs/skipped_files.log")       # See which files are skipped while running.

log_file = f"Logs/telecom_export.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

EXPORT_COLUMNS = {
    "call": [
        "customer_id", "imsi", "calling_party", "called_party", "billing_type", "service_type",
        "call_direction", "call_category", "call_duration", "call_start_time", "call_end_time",
        "available_limit", "deducted_limit", "available_onn_calls", "consumed_onn_calls",
        "available_ofn_calls", "consumed_ofn_calls", "customer_balance",
        "current_consumed_balance", "total_consumed_balance"
    ],
    "sms": [
        "customer_id", "imsi", "msisdn", "sms_receiver", "billing_type", "service_type",
        "call_direction", "call_category", "date", "available_limit", "deducted_limit",
        "available_onn_sms", "consumed_onn_sms", "available_ofn_sms", "consumed_ofn_sms"
    ],
    "data": [
        "customer_id", "imsi", "msisdn", "billing_type", "service_type", "call_direction",
        "call_category", "data_requested_time", "total_consumed_data", "available_limit",
        "deducted_limit", "available_data", "consumed_request"
    ]
}

column_map = {
    "data_requested_time":"timestamp",
    "call_start_ts": "timestamp",
    "callStartTs": "timestamp",
    "call_end_ts": "call_end_time",
    "callEndTs": "call_end_time",
    "calling_party": "msisdn",
    "available_limit": "balance",
    "deducted_limit": "charges",
    "called_party":"receiver",
    "sms_receiver":"receiver",
    "calledStationId":"receiver",    
}

def load_ingestion_tracker():
    if Path(TRACKER_FILE).exists():
        with open(TRACKER_FILE, "r") as f:
            return yaml.safe_load(f) or {}
    return {}

def save_ingestion_tracker(tracker):
    with open(TRACKER_FILE, "w") as f:
        yaml.safe_dump(tracker, f)

def infer_direction_subtype(path_parts):
    parts = [p.lower() for p in path_parts]
    if any(p in ["incoming call", "incoming sms"] for p in parts):
        return "incoming"
    elif any(p in ["outgoing call", "outgoing sms"] for p in parts):
        return "outgoing"
    elif "data" in parts:
        return "data"

def infer_category_subtype(path_parts):
    parts = [p.lower() for p in path_parts]
    if "off_net" in parts:
        return "offnet"
    return "onnet"

def infer_billing_type(file_path):
    path_str = str(file_path).lower()
    if "prepaid accounts" in path_str:
        return "prepaid"
    elif "postpaid accounts" in path_str:
        return "postpaid"

def infer_service_tag(file_path):
    path_str = str(file_path).lower()
    if "/call/" in path_str or "call" in path_str:
        return "call"
    elif "/sms/" in path_str or "sms" in path_str:
        return "sms"
    elif "/data/" in path_str or "data" in path_str:
        return "data"

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

def remove_trailing_date_folder(path: Path) -> str:
    parts = list(path.parts)
    last = parts[-1]
    try:
        if len(last) == 10:
            datetime.strptime(last, "%Y-%m-%d")
        elif len(last) == 7:
            datetime.strptime(last, "%Y-%m")
        elif len(last) == 4:
            datetime.strptime(last, "%Y")
        else:
            return str(path)
        return str(Path(*parts[:-1]))
    except ValueError:
        return str(path)


def parse_file(file_path, billing_type, service_tag):
    records = []
    path_parts = file_path.parts
    direction = infer_direction_subtype(path_parts)
    category = infer_category_subtype(path_parts)

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                decoder = json.JSONDecoder()
                idx = 0
                while idx < len(line):
                    try:
                        obj, offset = decoder.raw_decode(line[idx:])
                        obj = {column_map.get(k, k): v for k, v in obj.items()}
                        obj["billing_type"] = billing_type  # Prepaid | Postpaid
                        obj["service_type"] = service_tag   # Call | Data | SMS
                        obj["call_direction"] = direction   # Incoming | Outgoing | Missed
                        obj["call_category"] = category     # Offnet | Onnet

                        # Add Scope
                        parts = [p.lower() for p in path_parts]
                        if "international call" in parts:
                            obj["scope"] = "international"
                        elif "cug call" in parts:
                            obj["scope"] = "cug"
                        elif "data" in parts:
                            obj["scope"] = "data"
                        elif any(p in ["sms"] for p in parts):
                            obj["scope"] = "sms"
                        elif any(p not in ["international call", "cug call", "data", "sms"] for p in parts):
                            obj["scope"] = "national"

                        if service_tag == "sms":
                            obj["date"] = file_path.parent.name

                        records.append(obj)
                        idx += offset
                        while idx < len(line) and line[idx].isspace():
                            idx += 1
                    except json.JSONDecodeError:
                        idx += 1
                    except Exception:
                        break
    except Exception as e:
        logging.error(f" {file_path}: {e}")
    return records

def collect_and_group(root_dir, include_categories, use_tracker=True):
    root_path = Path(root_dir)
    if not root_path.exists() or not root_path.is_dir():
        logging.error(f"Root directory does not exist or is not a directory: {root_dir}")
        sys.exit(1)

    normalized_categories = [cat.lower() for cat in include_categories]
    tag_to_records = defaultdict(list)
    processed_files = 0
    latest_dates = {}
    tracker = load_ingestion_tracker() if use_tracker else {}
    last_folder = None

    for file_path in root_path.rglob("*.txt"):
        path_str = str(file_path).lower()
        folder_key = remove_trailing_date_folder(file_path.parent)

        if not any(cat in path_str for cat in normalized_categories):
            logging.debug(f"{file_path}, category_filter")
            continue

        billing_type = infer_billing_type(file_path)
        service_tag = infer_service_tag(file_path)
        direction = infer_direction_subtype(file_path.parts)
        final_tag = f"{billing_type}_{service_tag}"

        # Logging folders
        if folder_key != last_folder:
            direction = infer_direction_subtype(file_path.parts)
            logging.info(f"Processing folder: {folder_key} (direction: {direction})")
            last_folder = folder_key
        
        # Only allow outgoing call, outgoing sms, or data
        if not (
            (service_tag == "call" and direction == "outgoing") or
            (service_tag == "sms" and direction == "outgoing") or
            (service_tag == "data")
        ):
            logging.debug(f"Skipping non-usage file: {file_path} (tag={final_tag}, direction={direction})")
            continue

        file_date = extract_date_from_path(file_path)
        last_date = tracker.get(folder_key)

        if file_date and file_date < LAUNCH_DAY:
            logging.debug(f"{file_path}, pre_launch:{file_date}")
            continue

        if use_tracker and last_date:
            try:
                last_date_dt = datetime.strptime(last_date, "%Y-%m-%d").date()
                if file_date and file_date <= last_date_dt:
                    logging.debug(f"{file_path}, tracker_date:{last_date}")
                    continue
            except Exception as e:
                logging.debug(f"{file_path}, tracker_date_parse_error:{e}")

        try:
            records = parse_file(file_path, billing_type, service_tag)
            if records:
                tag_to_records[final_tag].extend(records)
        except Exception as e:
            logging.warning(f"{file_path}, parse_error:{e}")
            continue

        if file_date:
            prev = latest_dates.get(folder_key)
            if not prev or file_date > prev:
                latest_dates[folder_key] = file_date
        processed_files += 1

    logging.info(f"Processed {processed_files} files in total.")
    save_ingestion_tracker({k: v.strftime("%Y-%m-%d") for k, v in latest_dates.items()})
    return tag_to_records



# def collect_and_group(root_dir, include_categories, use_tracker=True):
#     root_path = Path(root_dir)
#     if not root_path.exists() or not root_path.is_dir():
#         logging.error(f"Root directory does not exist or is not a directory: {root_dir}")
#         sys.exit(1)

#     normalized_categories = [cat.lower() for cat in include_categories]
#     tag_to_records = defaultdict(list)
#     processed_files = 0

#     # Always track latest dates seen, even in --no-tracker mode
#     latest_dates = {}

#     # Load tracker if using it for filtering
#     tracker = load_ingestion_tracker() if use_tracker else {}
#     last_folder = None

#     for file_path in root_path.rglob("*.txt"):
#         path_str = str(file_path).lower()
#         folder_key = remove_trailing_date_folder(file_path.parent)

#         # Category filtering
#         if not any(cat in path_str for cat in normalized_categories):
#             logging.error(f"{file_path}, category_filter")
#             continue

#         # Logging folders
#         if folder_key != last_folder:
#             direction = infer_direction_subtype(file_path.parts)
#             if direction == "outgoing" or direction == "data":
#                 logging.info(f"Processing folder: {folder_key} (direction: {direction})")
#             else:
#                 last_folder = folder_key
#                 break
#             last_folder = folder_key

#         billing_type = infer_billing_type(file_path)
#         service_tag = infer_service_tag(file_path)
#         final_tag = f"{billing_type}_{service_tag}"

#         file_date = extract_date_from_path(file_path)
#         last_date = tracker.get(folder_key)

#         if file_date and file_date < LaunchDay:
#             logging.info(f"{file_path}", f"pre_launch:{file_date}")
#             continue

        
#         # Use tracker to skip already-processed files
#         if use_tracker and last_date:
#             try:
#                 last_date_dt = datetime.strptime(last_date, "%Y-%m-%d").date()
#                 if file_date and file_date <= last_date_dt:
#                     logging.info(f"{file_path}", f"tracker_date:{last_date}")
#                     continue
#             except Exception as e:
#                 logging.info(f"{file_path}", f"tracker_date_parse_error:{e}")
#                 pass

#         try:
#             records = parse_file(file_path, billing_type, service_tag)
#             if records:
#                 tag_to_records[final_tag].extend(records)
#         except Exception as e:
#             logging.info(f"{file_path}", f"parse_error:{e}")
#             continue

#         # Update the latest date for this folder (tracker build)
#         if file_date:
#             prev = latest_dates.get(folder_key)
#             if not prev or file_date > prev:
#                 latest_dates[folder_key] = file_date
#         processed_files += 1

#     logging.info(f"Processed {processed_files} files in total.")

#     # Always save tracker based on what was processed, even if --no-tracker was used
#     save_ingestion_tracker({
#         k: v.strftime("%Y-%m-%d") for k, v in latest_dates.items()
#     })

#     return tag_to_records

def export_to_csv(grouped_records, outdir):
    for tag, records in grouped_records.items():
        try:
            df = pd.DataFrame(records)
            service_type = tag.split("_")[1]  # Extract "call", "sms", or "data"
            if service_type in EXPORT_COLUMNS:
                df = df[[col for col in EXPORT_COLUMNS[service_type] if col in df.columns]]
            else:
                logging.warning(f"[EXPORT] Unknown service tag: {tag}")
            outfile = Path(outdir) / f"{tag}.csv"
            df.to_csv(outfile, index=False)
            logging.info(f"Wrote {tag} to CSV: {outfile} ({len(df)} rows)")
        except Exception as e:
            logging.error(f"Failed to export {tag} to CSV: {e}")


# def export_to_csv(grouped_records, outdir):
#     for tag, records in grouped_records.items():
#         try:
#             df = pd.DataFrame(records)
#             outfile = Path(outdir) / f"{tag}.csv"
#             df.to_csv(outfile, index=False)
#             logging.info(f"Wrote {tag} to CSV: {outfile} ({len(df)} rows)")
#         except Exception as e:
#             logging.error(f"Failed to export {tag} to CSV: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("root_dir", help="Root folder containing all service .txt files")
    parser.add_argument("--outdir", default="./output", help="Where to write output files")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    parser.add_argument("--no-tracker", action="store_true", help="Disable ingestion tracker (process all files)")
    args = parser.parse_args()

    try:
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        Path(args.outdir).mkdir(parents=True, exist_ok=True)

        grouped = collect_and_group(
            args.root_dir,
            config.get("include_categories", []),
            use_tracker=not args.no_tracker
        )

        if not grouped:
            logging.info(f"No records found to export.")
        else:
            export_to_csv(grouped, args.outdir)

    except Exception as e:
        logging.error(f"Script failed: {e}")

