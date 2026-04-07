import pandas as pd
import re
import unicodedata
from datetime import datetime


def norm(s):
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def transform_quickcheck_simple(df_raw):

    has_check_type_col = "Check Type" in df_raw.columns

    question_cols = [c for c in df_raw.columns if re.match(r'^\s*\d+\.\d+', str(c))]
    all_comment_cols = [c for c in df_raw.columns if 'comment' in c.lower()]

    overall_eval_cols = [c for c in df_raw.columns if "overall evaluation" in c.lower()]
    overall_eval_col = overall_eval_cols[0] if overall_eval_cols else None

    comment_norm = {c: norm(c) for c in all_comment_cols}

    rows = []

    for _, row in df_raw.iterrows():

        result_id = row.get('Record Number')
        submission_time = row.get('Submission Time')

        if has_check_type_col:
            check_type = row.get("Check Type")
            check_type = "Self-check" if pd.isna(check_type) else str(check_type).strip()
        else:
            check_type = "Self-check"

        for qcol in question_cols:

            m = re.match(r'^\s*(\d+\.\d+)\s*(.*)$', str(qcol))
            qid = m.group(1) if m else ""
            qtext = m.group(2).strip() if m else str(qcol)
            qtext_norm = norm(qtext)

            matched_comments = [c for c, cn in comment_norm.items() if qtext_norm and qtext_norm in cn]

            comment_val = None
            for c in matched_comments:
                v = row.get(c)
                if pd.notna(v) and str(v).strip() != "":
                    comment_val = str(v).strip()
                    break

            result_val = row.get(qcol)

            if pd.isna(result_val) and comment_val is None:
                continue

            qtype_digit = qid.split('.')[0] if qid else ''
            qtype_map = {
                "1": "Sales",
                "2": "Delivery",
                "3": "Aftersales",
                "4": "Marketing&UserOperation",
                "5": "StaffDemeanor"
            }
            qtype = qtype_map.get(qtype_digit, "Other")

            rows.append({
                "RecordID": f"{result_id}_{qid}",
                "ResultID": result_id,
                "SubmissionTime": submission_time,
                "CheckType": check_type,
                "Region": row.get("Region"),
                "Country/Region": row.get("Country/region"),
                "City": row.get("City"),
                "StoreName": row.get("Store Name"),
                "Creator": row.get("Creator"),
                "QuestionID": qid,
                "QuestionType": qtype,
                "QuestionText": qtext,
                "Result": result_val,
                "Comment": comment_val
            })

        if overall_eval_col:
            overall_comment = row.get(overall_eval_col)
            if pd.notna(overall_comment):
                rows.append({
                    "RecordID": f"{result_id}_0.0",
                    "ResultID": result_id,
                    "SubmissionTime": submission_time,
                    "CheckType": check_type,
                    "Region": row.get("Region"),
                    "Country/Region": row.get("Country/region"),
                    "City": row.get("City"),
                    "StoreName": row.get("Store Name"),
                    "Creator": row.get("Creator"),
                    "QuestionID": "0.0",
                    "QuestionType": "Overall Evaluation",
                    "QuestionText": "Overall Evaluation",
                    "Result": None,
                    "Comment": str(overall_comment).strip()
                })

    df_long = pd.DataFrame(rows)

    if not df_long.empty:
        df_long = df_long.sort_values(by=['ResultID', 'QuestionID']).reset_index(drop=True)

        df_long["Comment_AutoTranslate"] = [
            '=IFERROR(GOOGLETRANSLATE(INDIRECT("M"&ROW()),"auto","en"),"")'
            for _ in range(len(df_long))
        ]

    return df_long


def build_fact_submission(df_raw, form_sheet_name=None):

    # Convert Submission Time to datetime first
    df_raw["Submission Time"] = pd.to_datetime(df_raw["Submission Time"])

    has_check_type_col = "Check Type" in df_raw.columns
    rows = []

    for _, r in df_raw.iterrows():
        submission_time = r.get("Submission Time")
        if has_check_type_col:
            check_type = r.get("Check Type")
            check_type = "Self-check" if pd.isna(check_type) else str(check_type).strip()
        else:
            check_type = "Self-check"

        rows.append({
            "SubmissionID": r.get("Record Number"),
            "SubmissionTime": submission_time,  # ✅ rename here immediately
            "CheckType": check_type,
            "Creator": r.get("Creator"),
            "Region": r.get("Region"),
            "Country/Region": r.get("Country/region"),
            "City": r.get("City"),
            "StoreName": r.get("Store Name"),
            "FormSheet": form_sheet_name,
        })

    df = pd.DataFrame(rows)

    # ✅ Now sort works
    df = df.sort_values("SubmissionTime")

    # Keep SubmissionTime as string for export
    df["SubmissionTime"] = df["SubmissionTime"].dt.strftime("%Y/%m/%d %H:%M:%S")

    return df


# ===== Your original coverage functions (UNCHANGED) =====
def build_dim_store_coverage(df_store_master, df_fact_submission, ref_month=None):
    """
    Build store-level coverage table using WIDE submission fact table.
    """

    df = df_store_master.copy()

    # -----------------------------
    # Rename store master columns
    # -----------------------------
    df = df.rename(columns={
        "序号": "StoreID",
        "Region": "Region",
        "大区": "MarketRegion",
        "Country": "Country",
        "City": "City",
        "Store Name": "StoreName",
        "Model": "Model",
        "Agent/Dealer/Distributor": "AgentDealerDistributor",
        "Store Function": "StoreFunction",
        "Open Time": "OpenTime",      # 🔹 renamed
        "Address": "Address"
    })

    # keep OpenTime as YYYYMM string
    df["OpenTime"] = df["OpenTime"].astype(str)

    # -----------------------------
    # Exclude temporary stores
    # -----------------------------
    df = df[~df["StoreFunction"].str.contains("Temporary", na=False)].copy()

    # -----------------------------
    # Open ≥ 3 months rule
    # -----------------------------
    open_dt = pd.to_datetime(df["OpenTime"], format="%Y%m", errors="coerce")
    today = pd.Timestamp.today().normalize()

    df["MonthsOpen"] = (
        (today.year - open_dt.dt.year) * 12
        + (today.month - open_dt.dt.month)
    )

    df["IsCountedStore"] = df["MonthsOpen"] >= 3

    # -----------------------------
    # Prepare submission data
    # -----------------------------
    sub = df_fact_submission.copy()
    sub["SubmissionTime"] = pd.to_datetime(sub["SubmissionTime"], errors="coerce")

    # Latest submission per store
    latest = (
        sub.sort_values("SubmissionTime")
           .groupby("StoreName", as_index=False)
           .last()
    )

    # -----------------------------
    # Submission this month flag
    # -----------------------------
    if ref_month is None:
        ref_month = today.to_period("M")

    latest["SubmittedThisMonth"] = (
        latest["SubmissionTime"].dt.to_period("M") == ref_month
    ).astype(int)

    # -----------------------------
    # Select columns to merge
    # -----------------------------
    latest = latest[[
        "StoreName",
        "SubmissionTime",
        "SubmittedThisMonth",
        "Sales_PassRate",
        "Delivery_PassRate",
        "Aftersales_PassRate",
        "Marketing_PassRate"
    ]]

    # -----------------------------
    # Merge back to store master
    # -----------------------------
    df = df.merge(latest, on="StoreName", how="left")
    df["SubmittedThisMonth"] = df["SubmittedThisMonth"].fillna(0).astype(int)

    # -----------------------------
    # Final column selection (IMPORTANT)
    # -----------------------------
    df = df[[
        "StoreID",
        "MarketRegion",
        "Region",
        "Country",
        "Model",
        "City",
        "StoreName",
        "AgentDealerDistributor",
        "StoreFunction",
        "OpenTime",
        "Address",
        "MonthsOpen",
        "IsCountedStore",
        "SubmissionTime",
        "SubmittedThisMonth",
        "Sales_PassRate",
        "Delivery_PassRate",
        "Aftersales_PassRate",
        "Marketing_PassRate"
    ]]

    return df



def build_country_coverage(df_store_coverage):
    """
    Build country-level store coverage table.
    """

    df = df_store_coverage.copy()

    # Only count eligible stores
    df = df[df["IsCountedStore"] == True]

    agg = (
        df.groupby(["Region", "MarketRegion", "Country"], as_index=False)
          .agg(
              TotalStores=("StoreID", "count"),
              CoveredStores=("SubmittedThisMonth", "sum"),
          )
    )

    agg["UncoveredStores"] = agg["TotalStores"] - agg["CoveredStores"]
    agg["CoverageRate"] = agg["CoveredStores"] / agg["TotalStores"]

    return agg


def build_country_coverage(df_store_coverage):
    """
    Build country-level store coverage table.
    """

    df = df_store_coverage.copy()

    # Only count eligible stores
    df = df[df["IsCountedStore"] == True]

    agg = (
        df.groupby(["Region", "MarketRegion", "Country"], as_index=False)
          .agg(
              TotalStores=("StoreID", "count"),
              CoveredStores=("SubmittedThisMonth", "sum"),
          )
    )

    agg["UncoveredStores"] = agg["TotalStores"] - agg["CoveredStores"]
    agg["CoverageRate"] = agg["CoveredStores"] / agg["TotalStores"]

    return agg


def build_region_coverage(df_store_coverage):
    """
    Build region-level store coverage table.
    """

    df = df_store_coverage.copy()

    # Only count eligible stores
    df = df[df["IsCountedStore"] == True]

    agg = (
        df.groupby(["Region"], as_index=False)
          .agg(
              TotalStores=("StoreID", "count"),
              CoveredStores=("SubmittedThisMonth", "sum"),
          )
    )

    agg["UncoveredStores"] = agg["TotalStores"] - agg["CoveredStores"]
    agg["CoverageRate"] = agg["CoveredStores"] / agg["TotalStores"]

    return agg



# =========================
# Main runner (FULL)
# =========================
def run_transformation(file_path):

    xls = pd.ExcelFile(file_path, engine="openpyxl")

    dfs_long = []
    dfs_submission = []

    for sheet in xls.sheet_names:
        if "Quick Check" in sheet:
            df_raw = pd.read_excel(xls, sheet_name=sheet)

            df_long = transform_quickcheck_simple(df_raw)
            df_long["FormSheet"] = sheet
            dfs_long.append(df_long)

            df_submission = build_fact_submission(df_raw, sheet)
            dfs_submission.append(df_submission)

    df_long_all = pd.concat(dfs_long, ignore_index=True)
    df_fact_submission_all = pd.concat(dfs_submission, ignore_index=True)

    df_store_master = pd.read_excel(
        xls,
        sheet_name="门店明细（引用）"
    )

    df_store_coverage = build_dim_store_coverage(
        df_store_master,
        df_fact_submission_all
    )

    df_country_coverage = build_country_coverage(df_store_coverage)
    df_region_coverage = build_region_coverage(df_store_coverage)

    return {
        "fact_submission": df_fact_submission_all,
        "fact_question": df_long_all,
        "store_coverage": df_store_coverage,
        "country_coverage": df_country_coverage,
        "region_coverage": df_region_coverage
    }
