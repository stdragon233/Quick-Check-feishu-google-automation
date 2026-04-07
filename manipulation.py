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
    """Wide -> long transformation
       + Safe CheckType handling
       + Row-level Google Sheets translation formula
       + Translation column BEFORE Comment
       + Support for Overall Evaluation (QuestionID = 0.0)
    """

    import re
    import unicodedata
    import pandas as pd

    def norm(s):
        if pd.isna(s):
            return ""
        s = str(s).lower().strip()
        s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
        s = re.sub(r'[^\w\s]', '', s)
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    has_check_type_col = "Check Type" in df_raw.columns

    question_cols = [c for c in df_raw.columns if re.match(r'^\s*\d+\.\d+', str(c))]
    all_comment_cols = [c for c in df_raw.columns if 'comment' in c.lower()]
    all_attach_cols = [c for c in df_raw.columns if 'attachment' in c.lower()]

    # ✅ Detect Overall Evaluation column
    overall_eval_cols = [c for c in df_raw.columns if "overall evaluation" in c.lower()]
    overall_eval_col = overall_eval_cols[0] if overall_eval_cols else None

    comment_norm = {c: norm(c) for c in all_comment_cols}
    attach_norm = {c: norm(c) for c in all_attach_cols}

    rows = []

    for idx, row in df_raw.iterrows():

        result_id = row.get('Record Number', None)
        submission_time = row.get('Submission Time', None)

        if has_check_type_col:
            check_type = row.get("Check Type")
            if pd.isna(check_type) or str(check_type).strip() == "":
                check_type = "Self-check"
            else:
                check_type = str(check_type).strip()
        else:
            check_type = "Self-check"

        # ==================================================
        # Normal Questions
        # ==================================================
        for qcol in question_cols:

            m = re.match(r'^\s*(\d+\.\d+)\s*(.*)$', str(qcol))
            qid = m.group(1) if m else ""
            qtext = m.group(2).strip() if m else str(qcol)
            qtext_norm = norm(qtext)

            matched_comments = [c for c, cn in comment_norm.items() if qtext_norm and qtext_norm in cn]
            no_pass_cols = [c for c in matched_comments if 'no pass' in c.lower() or 'nopass' in c.lower() or 'no_pass' in c.lower()]
            pass_cols = [c for c in matched_comments if 'pass' in c.lower() and c not in no_pass_cols]
            other_comments = [c for c in matched_comments if c not in no_pass_cols and c not in pass_cols]

            comment_val = None
            for c in (no_pass_cols + pass_cols + other_comments):
                if c in df_raw.columns:
                    v = row.get(c)
                    if pd.notna(v) and str(v).strip() != "":
                        comment_val = str(v).strip()
                        break

            result_val = row.get(qcol, None)
            has_result = not (pd.isna(result_val) or str(result_val).strip() == "")
            has_comment = comment_val is not None

            if not (has_result or has_comment):
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

            record_id = f"{result_id}_{qid}" if (result_id and qid) else (result_id or qid or "")

            rows.append({
                "RecordID": record_id,
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

        # ==================================================
        # Overall Evaluation (Special Case)
        # ==================================================
        if overall_eval_col:
            overall_comment = row.get(overall_eval_col)

            if pd.notna(overall_comment) and str(overall_comment).strip() != "":

                record_id = f"{result_id}_0.0" if result_id else "0.0"

                rows.append({
                    "RecordID": record_id,
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

        df_long = df_long.sort_values(by=['ResultID','QuestionID']).reset_index(drop=True)

        comment_index = df_long.columns.get_loc("Comment")
        df_long.insert(comment_index, "Comment_AutoTranslate", "")

        comment_col_index = df_long.columns.get_loc("Comment")

        def excel_col_letter(n):
            result = ""
            while n >= 0:
                result = chr(n % 26 + 65) + result
                n = n // 26 - 1
            return result

        comment_letter = excel_col_letter(comment_col_index)

        for i in range(len(df_long)):
            formula = f'=IFERROR(GOOGLETRANSLATE(INDIRECT("{comment_letter}"&ROW()),"auto","en"),"")'
            df_long.at[i, "Comment_AutoTranslate"] = formula

    return df_long


def build_fact_submission(df_raw, form_sheet_name=None):
    """
    Build 1-row-per-submission fact table using form-calculated metrics.
    """

    # ensure Submission Time is datetime (used internally only)
    df_raw["Submission Time"] = pd.to_datetime(df_raw["Submission Time"])

    # NEW ✅ detect if Check Type column exists
    has_check_type_col = "Check Type" in df_raw.columns

    section_map = {
        "Sales": {
            "rate": "Sales Pass Rate",
            "pass": "Sales Pass Count",
            "nopass": "Sales NoPass Count",
            "na": "Sales NA Count"
        },
        "Delivery": {
            "rate": "Delivery Pass Rate",
            "pass": "Delivery Pass Count",
            "nopass": "Delivery NoPass Count",
            "na": "Delivery NA Count"
        },
        "Aftersales": {
            "rate": "Aftersales Pass Rate",
            "pass": "Aftersales Pass Count",
            "nopass": "Aftersales NoPass Count",
            "na": "Aftersales NA Count"
        },
        "Marketing": {
            "rate": "Marketing Pass Rate",
            "pass": "Marketing Pass Count",
            "nopass": "Marketing NoPass Count",
            "na": "Marketing NA Count"
        }
    }

    rows = []

    for _, r in df_raw.iterrows():

        submission_time = r.get("Submission Time")

        # NEW ✅ safely extract Check Type
        if has_check_type_col:
            check_type = r.get("Check Type")
            if pd.isna(check_type) or str(check_type).strip() == "":
                check_type = "Self-check"
            else:
                check_type = str(check_type).strip()
        else:
            check_type = "Self-check"

        row = {
            "SubmissionID": r.get("Record Number"),
            "SubmissionTime": submission_time,
            "CheckType": check_type,  # NEW ✅ added here
            "Creator": r.get("Creator"),
            "Region": r.get("Region"),
            "Country/Region": r.get("Country/region"),
            "City": r.get("City"),
            "StoreName": r.get("Store Name"),
            "FormSheet": form_sheet_name,

            # derived columns
            "YearMonth": submission_time.strftime("%Y%m"),
            "Quarter": f"{submission_time.year} Q{submission_time.quarter}",
            "Submission Count": 1
        }

        # section metrics + section submission counts
        for section, cols in section_map.items():
            prefix = section.replace(" ", "")

            pass_cnt = r.get(cols["pass"]) or 0
            nopass_cnt = r.get(cols["nopass"]) or 0
            na_cnt = r.get(cols["na"]) or 0

            row[f"{prefix}_PassRate"] = r.get(cols["rate"])
            row[f"{prefix}_PassCount"] = pass_cnt
            row[f"{prefix}_NoPassCount"] = nopass_cnt
            row[f"{prefix}_NACount"] = na_cnt

            row[f"{prefix}_SubmissionCount"] = (
                1 if (pass_cnt + nopass_cnt + na_cnt) > 0 else 0
            )

        rows.append(row)

    # build dataframe
    df = pd.DataFrame(rows)

    # force added columns to the very end
    extra_cols = [
        "YearMonth",
        "Quarter",
        "Submission Count",
        "Sales_SubmissionCount",
        "Delivery_SubmissionCount",
        "Aftersales_SubmissionCount",
        "Marketing_SubmissionCount"
    ]
    df = df[[c for c in df.columns if c not in extra_cols] + extra_cols]

    # FINAL SORT (earliest → latest)
    df = df.sort_values("SubmissionTime", ascending=True)

    # restore SubmissionTime to original string format for export
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
    
    # =========================================================
    # 1. Load Excel (multiple Quick Check sheets)
    # =========================================================
   
    xls = pd.ExcelFile(file_path, engine="openpyxl")

    dfs_long = []
    dfs_submission = []

    for sheet in xls.sheet_names:
        if "Quick Check" in sheet:
            try:
                df_raw = pd.read_excel(xls, sheet_name=sheet, header=0)
    
                df_long = transform_quickcheck_simple(df_raw)
                df_long["FormSheet"] = sheet
                dfs_long.append(df_long)
    
                df_submission = build_fact_submission(df_raw, sheet)
                dfs_submission.append(df_submission)
    
                print(f"✅ Loaded sheet: {sheet}")
            except Exception as e:
                print(f"❌ Failed sheet: {sheet}")
                print(e)

    df_long_all = pd.concat(dfs_long, ignore_index=True)
    df_fact_submission_all = pd.concat(dfs_submission, ignore_index=True)
    
    # =========================================================
    # 2. Load Store Master (dim table)
    # =========================================================
    
    df_store_master = pd.read_excel(
        xls,
        sheet_name="门店明细（引用）",
        header=0
    )

    # =========================================================
    # 3. Build Coverage Tables
    # =========================================================

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
