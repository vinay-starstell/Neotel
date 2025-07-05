# dashboard.py
import pandas as pd
import logging
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.cell.cell import Cell
from pathlib import Path
import numpy as np
from datetime import datetime

log_file = f"Logs/Dashboard01.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
def standardize_columns(df):
    """Lowercase and strip all column names for consistency."""
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df

def safe_index_to_str(series):
    s = series.copy()
    s.index = s.index.map(str)
    return s.groupby(s.index).sum()

def load_and_standardize(path, dtype, column_map=None):
    df = pd.read_csv(path, dtype=dtype, low_memory=False)
    df = standardize_columns(df)
    if column_map:
        df.rename(columns={k.lower(): v for k, v in column_map.items()}, inplace=True)
    # If this is an SMS file and 'date' is not present, extract from path
    if "sms" in str(path).lower() and "date" not in df.columns:
        # Try to extract date from parent folder name
        date_str = Path(path).parent.name
        try:
            # Validate/parse date
            date_val = pd.to_datetime(date_str, errors="coerce").date()
            df["date"] = date_val
        except Exception:
            df["date"] = pd.NaT
    return df

def load_all_inputs(base_dir):
    logging.info("Loading all input files...")
   
    call_dtypes = {
        'customer_id': 'str', 'imsi': 'str', 'calling_party': 'str', 'called_party': 'str',
        'billing_type': 'str', 'service_type': 'str', 'call_direction': 'str', 'call_category': 'str',
        'call_duration': 'float', "call_start_time": 'str', "call_end_time": 'str', 
        "available_limit": 'float', "deducted_limit": 'float',"available_onn_calls": 'float', 
        "consumed_onn_calls": 'float',"available_ofn_calls": 'float',"consumed_ofn_calls": 'float',
        "customer_balance": 'float',"current_consumed_balance":'float',"total_consumed_balance":'float'
    }
    sms_dtypes = {
        'customer_id': 'str', 'imsi': 'str', 'msisdn': 'str', "sms_receiver": 'str', 'sms_request': 'str', 'billing_type': 'str', 
        'service_type': 'str', 'call_direction': 'str', 'call_category': 'str','date': 'str', 
        "available_limit": 'float', "deducted_limit": 'float',"available_onn_sms": 'float', 
        "consumed_onn_sms": 'float',"available_ofn_sms": 'float',"consumed_ofn_sms": 'float'
    }
    data_dtypes = {
        'customer_id': 'str', 'imsi': 'str',  'msisdn': 'str', 'billing_type': 'str', 'service_type': 'str', 
        'call_direction': 'str', 'call_category': 'str', 'data_requested_time': 'str', 
        'total_consumed_data': 'float', "available_limit": 'float', "deducted_limit": 'float', 
        "available_data": 'float', "consumed_request": 'float', 
    }

    dfs = {
        "calls": pd.concat([
            load_and_standardize(base_dir / "postpaid_call.csv", call_dtypes),
            load_and_standardize(base_dir / "prepaid_call.csv", call_dtypes)
        ], ignore_index=True),
        "sms": pd.concat([
            load_and_standardize(base_dir / "postpaid_sms.csv", sms_dtypes),
            load_and_standardize(base_dir / "prepaid_sms.csv", sms_dtypes)
        ], ignore_index=True),
        "data": pd.concat([
            load_and_standardize(base_dir / "postpaid_data.csv", data_dtypes),
            load_and_standardize(base_dir / "prepaid_data.csv", data_dtypes)
        ], ignore_index=True),
        "sim_inventory": standardize_columns(pd.read_csv(base_dir / "sim_inventory_export2.csv", dayfirst=True, parse_dates=["allocation_date"])),
        "activations": standardize_columns(pd.read_csv(base_dir / "ActivatedSIMcustomer.csv", parse_dates=["create_date"])),
        "partners": standardize_columns(pd.read_csv(base_dir / "partner.csv")),
    }
    
    # Convert sim_inventory and activations dates
    if "allocation_date" in dfs["sim_inventory"].columns:
        dfs["sim_inventory"]["allocation_date"] = pd.to_datetime(dfs["sim_inventory"]["allocation_date"], errors="coerce", dayfirst=True).dt.date
    if "create_date" in dfs["activations"].columns:
        dfs["activations"]["create_date"] = pd.to_datetime(dfs["activations"]["create_date"], errors="coerce", dayfirst=True).dt.date
    logging.info("Finished loading files.")
    return dfs

def get_user_column(df):
    for col in ["msisdn", "customer_id", "imsi"]:
        if col in df.columns:
            return df[["date", col]].dropna()
    return df[["date"]].assign(dummy_user="unknown")

def flexible_category_match(series, patterns):
    """Return a boolean Series matching any of the given patterns (case-insensitive, flexible)."""
    regex = "|".join([fr"\\b{p.replace('_', '[_-]?')}\\b" for p in patterns])
    return series.astype(str).str.lower().str.contains(regex, na=False, regex=True)

def summarize_sim(df_inventory, df_activation):
    logging.info("Summarizing SIM data...")
    summaries = {}
    inventory_daily = df_inventory.copy()
    if "allocation_date" in inventory_daily.columns:
        sold = inventory_daily.groupby("allocation_date").size().rename("SIMs Sold")
        sold = sold.to_frame().T
        sold["Grand Total"] = sold.sum(axis=1)
        sold = sold[["Grand Total"] + sorted([c for c in sold.columns if c != "Grand Total"], reverse=True)]
        summaries["SIMs Sold"] = sold
    if "billing_type" not in df_inventory.columns:
        df_inventory["billing_type"] = "Prepaid"
    for billing_type in ["prepaid", "postpaid"]:
        df_act = df_activation[df_activation["customertype"].str.lower() == billing_type]
        if df_act.empty:
            logging.info(f"No activation data for {billing_type}.")
            continue
        activated = df_act.groupby("create_date").size().rename("SIMs Activated")
        activated = activated.to_frame().T
        activated["Grand Total"] = activated.sum(axis=1)
        activated = activated[["Grand Total"] + sorted([c for c in activated.columns if c != "Grand Total"], reverse=True)]
        summaries[billing_type] = activated
    return summaries


def summarize_usage(df_calls, df_sms, df_data):
    logging.info("Summarizing usage data...")
    summaries = {}
    for billing_type in ["prepaid", "postpaid"]:
        # Filter for current billing type
        calls = df_calls[df_calls["billing_type"] == billing_type].copy() if "billing_type" in df_calls.columns else pd.DataFrame()
        sms = df_sms[df_sms["billing_type"] == billing_type].copy() if "billing_type" in df_sms.columns else pd.DataFrame()
        data = df_data[df_data["billing_type"] == billing_type].copy() if "billing_type" in df_data.columns else pd.DataFrame()
                  
        # Parse date columns
        if "call_start_time" in calls.columns:
            calls["date"] = pd.to_datetime(calls["call_start_time"], errors="coerce").dt.date
        
        # Try to create a 'date' column from the most likely date columns
        if ("date", "sms_date", "data_requested_time") in sms.columns:
            sms["date"] = pd.to_datetime(sms["date"], errors="coerce").dt.date
            
        if "data_requested_time" in data.columns:
            data["data_requested_time"] = pd.to_datetime(data["data_requested_time"], errors="coerce").dt.date
        
        # Filter onnet/offnet outgoing calls/sms
        onnet_outgoing = calls[calls["category"].str.contains("onnet", na=False)] if "category" in calls.columns else pd.DataFrame()
        offnet_outgoing = calls[calls["category"].str.contains("offnet", na=False)] if "category" in calls.columns else pd.DataFrame()
        # sms_onnet = sms[sms["category"] == "onnet"] if "category" in sms.columns else pd.DataFrame()
        # sms_offnet = sms[sms["category"] == "offnet"] if "category" in sms.columns else pd.DataFrame()
        sms_onnet = sms[sms["category"].str.contains("onnet", na=False)] if "category" in sms.columns else pd.DataFrame()
        sms_offnet = sms[sms["category"].str.contains("offnet", na=False)] if "category" in sms.columns else pd.DataFrame()
        
        
        # Aggregate metrics (safe for empty DataFrames)
        if "consumed_onn_calls" in calls.columns and "consumed_ofn_calls" in calls.columns and not calls.empty:
            voice_outgoing = (calls.groupby("date")["consumed_onn_calls"].sum() + calls.groupby("date")["consumed_ofn_calls"].sum()) / 60
        else:
            voice_outgoing = pd.Series(dtype=float)
        onnet_calls = onnet_outgoing.groupby("date")["consumed_onn_calls"].sum() / 60 if "consumed_onn_calls" in onnet_outgoing.columns and not onnet_outgoing.empty else pd.Series(dtype=float)
        offnet_calls = offnet_outgoing.groupby("date")["consumed_ofn_calls"].sum() / 60 if "consumed_ofn_calls" in offnet_outgoing.columns and not offnet_outgoing.empty else pd.Series(dtype=float)
        
        
        if "consumed_onn_sms" in sms.columns and "consumed_ofn_sms" in sms.columns and not sms.empty:
            sms_outgoing = (sms_onnet.groupby("date")["consumed_onn_sms"].sum() + sms_offnet.groupby("date")["consumed_ofn_sms"].sum())
        else:
            sms_outgoing = pd.Series(dtype=float)
        onn_sms = sms_onnet.groupby("date")["consumed_onn_sms"].sum() if "consumed_onn_sms" in sms_onnet.columns and not sms_onnet.empty else pd.Series(dtype=float)
        ofn_sms = sms_offnet.groupby("date")["consumed_ofn_sms"].sum() if "consumed_ofn_sms" in sms_offnet.columns and not sms_offnet.empty else pd.Series(dtype=float)
        
        
        total_data = data.groupby("date")["consumed_data"].sum() / 1024**3 if "consumed_data" in data.columns and not data.empty else pd.Series(dtype=float)
        
        # Active users
        user_calls = get_user_column(calls) if not calls.empty else pd.DataFrame()
        user_sms = get_user_column(sms) if not sms.empty else pd.DataFrame()
        user_data = get_user_column(data) if not data.empty else pd.DataFrame()
        active_users = pd.concat([user_calls, user_sms, user_data]).drop_duplicates().groupby("date").size() if not (user_calls.empty and user_sms.empty and user_data.empty) else pd.Series(dtype=int)
        
        summary = pd.DataFrame({
            "Voice [Outgoing Mins]": safe_index_to_str(voice_outgoing),
            "On-net [Outgoing]": safe_index_to_str(onnet_calls),
            "Off-net [Outgoing]": safe_index_to_str(offnet_calls),
            "SMS [Outgoing Count]": safe_index_to_str(sms_outgoing),
            "on-net [SMS]" :safe_index_to_str(onn_sms),
            "off-net [SMS]" :safe_index_to_str(ofn_sms),
            "Data Usage[GB]" :safe_index_to_str(total_data),
            "Active Users": safe_index_to_str(active_users),
        }).fillna(0).T
        
        if summary.empty:
            continue
        summary["Grand Total"] = summary.sum(axis=1)
        summary.columns = [str(c) for c in summary.columns]  # Ensure all columns are strings
        summary = summary[["Grand Total"] + sorted([c for c in summary.columns if c != "Grand Total"], reverse=True)]
        
        summaries[billing_type] = summary
    return summaries

def summarize_partner_activations(df_activations, df_partners):
    logging.info("Summarizing partner SIM activations...")
    summaries = {}
    for billing_type in ["prepaid", "postpaid"]:
        df = df_activations[df_activations["customertype"].str.lower() == billing_type].copy()
        if df.empty:
            logging.info(f"No partner activation data for {billing_type}.")
            continue
        df["partner_id"] = df["partner_id"].astype(str)
        df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce", dayfirst=True).dt.date
        df_partners["id"] = df_partners["id"].astype(str)
        merged = df.merge(df_partners, left_on="partner_id", right_on="id", how="left")
        if "business_name" not in merged.columns:
            merged["business_name"] = "Unknown Partner"
        daily = merged.groupby(["business_name", "create_date"]).size().unstack(fill_value=0).sort_index(axis=1)
        daily["Grand Total"] = daily.sum(axis=1)
        daily = daily[["Grand Total"] + sorted([c for c in daily.columns if c != "Grand Total"], reverse=True)]
        daily = daily.loc[~(daily == 0).all(axis=1)]
        summaries[billing_type] = daily
    return summaries

def generate_historical_summaries(dfs):
    import datetime
    logging.info("Generating historical summaries...")
    result = {}
    usage_dict = summarize_usage(dfs["calls"], dfs["sms"], dfs["data"])
    current_year = datetime.datetime.now().year
    for billing_type in ["prepaid", "postpaid"]:
        if billing_type not in usage_dict:
            continue
        usage = usage_dict[billing_type].T.copy()
        usage.index = pd.to_datetime(usage.index, errors="coerce", format="%Y-%m-%d")
        usage = usage.dropna()
        if usage.empty:
            continue
        usage_current_year = usage[usage.index.year == current_year]
        def fmt(df, freq, fmt_str):
            df = df.resample(freq).sum()
            df["sort_key"] = df.index
            df.index = df.index.strftime(fmt_str)
            df = df.sort_values("sort_key", ascending=False).drop(columns="sort_key")
            return df.T
        result[f"{billing_type} - Weekly Summary"] = fmt(usage_current_year, "W-MON", "W%W(%d%b)")
        result[f"{billing_type} - Monthly Summary"] = fmt(usage, "ME", "%b %Y")
        result[f"{billing_type} - Yearly Summary"] = fmt(usage, "YE", "%Y")
    return result

def write_table(wb, sheet_name, content_dict):
    logging.info(f"Writing sheet: {sheet_name}")
    ws = wb.create_sheet(sheet_name)
    row_offset = 1
    for section, df in content_dict.items():
        ws.merge_cells(start_row=row_offset, start_column=1, end_row=row_offset, end_column=df.shape[1] + 1)
        cell = ws.cell(row=row_offset, column=1, value=section)
        cell.fill = PatternFill(start_color="FF4958A6", end_color="FF4958A6", fill_type="solid")
        cell.font = Font(color="FFFFFF", bold=True, size=8)
        row_offset += 1
        _write_df(ws, df, row_offset)
        row_offset += len(df) + 2
    _auto_width(ws)

def _write_df(ws, df, row_offset):
    df.index.name = None
    rows = list(dataframe_to_rows(df, index=True, header=True))
    if len(rows) > 0 and all(x is None for x in rows[0]):
        rows = rows[1:]
    for r_idx, row in enumerate(rows):
        for c_idx, val in enumerate(row, start=1):
            cell = ws.cell(row=row_offset + r_idx, column=c_idx, value=int(round(val)) if isinstance(val, float) else val)
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.font = Font(size=8)
            if r_idx == 0:
                cell.fill = PatternFill(start_color="FF4958A6", end_color="FF4958A6", fill_type="solid")
                cell.font = Font(color="FFFFFF", bold=True, size=8)

def _auto_width(ws):
    fixed_width = 8.6
    for i, col in enumerate(ws.columns, start=1):
        col_letter = get_column_letter(i)
        values = [cell.value for cell in col if isinstance(cell, Cell) and cell.value]
        if values:
            max_len = max(len(str(v)) for v in values)
            if i == 1:
                ws.column_dimensions[col_letter].width = min(max_len + 2, 60)
            else:
                ws.column_dimensions[col_letter].width = fixed_width

def build_dashboard(dfs, out_path):
    logging.info("Building dashboard...")
    wb = Workbook()
    wb.remove(wb.active)
    write_table(wb, "Sim Summary", summarize_sim(dfs["sim_inventory"], dfs["activations"]))
    write_table(wb, "Usage Summary", summarize_usage(dfs["calls"], dfs["sms"], dfs["data"]))
    write_table(wb, "Partner SIM Activations", summarize_partner_activations(dfs["activations"], dfs["partners"]))
    write_table(wb, "Performance Summary", generate_historical_summaries(dfs))
    wb.save(out_path)
    logging.info(f"Dashboard saved to {out_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="./Files", help="Path to folder containing input files")
    parser.add_argument("--out", default="Dashboard01.xlsx", help="Path to output Excel file")
    args = parser.parse_args()
    base_dir = Path(args.base)
    dfs = load_all_inputs(base_dir)
    build_dashboard(dfs, args.out)
