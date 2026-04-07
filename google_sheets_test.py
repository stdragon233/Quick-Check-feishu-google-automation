import os
print("RUNNING FILE:", __file__)

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json


print("🚀 Script started")

from manipulation import run_transformation
print("✅ Imported manipulation.py")

# ===== Auth =====
scope = ["https://www.googleapis.com/auth/spreadsheets"]
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)
print("✅ Auth success")

# ===== Open Sheet =====
SHEET_ID = "1mc4JNOVy9dPZEu2Xc-c9duRP6_otwNg2hbnGeZw9wV4"
spreadsheet = client.open_by_key(SHEET_ID)
print("✅ Opened Google Sheet")

# ===== Check file exists =====
file_path = "Red Carpet Quick Check 20260401.xlsx"

if not os.path.exists(file_path):
    print("❌ FILE NOT FOUND:", file_path)
    exit()

print("✅ Excel file found")

# ===== Run transformation =====
data = run_transformation(file_path)
print("✅ Transformation done")

# ===== Upload helper =====
def upload_df(sheet_name, df):
    print(f"Uploading: {sheet_name}, rows={len(df)}")
    
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        sheet.clear()
    except:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")

    # --- FIX STARTS HERE ---
    # Convert all columns to string to handle Timestamps and NaNs for JSON serialization
    df_clean = df.fillna("").astype(str)
    
    # Prepare the data: list of headers + list of row values
    data_to_upload = [df_clean.columns.values.tolist()] + df_clean.values.tolist()
    
    sheet.update(data_to_upload, value_input_option='USER_ENTERED')
    # --- FIX ENDS HERE ---

# ===== Upload ALL =====
upload_df("fact_submission", data["fact_submission"])
upload_df("fact_question_long", data["fact_question"])
upload_df("store_coverage", data["store_coverage"])
upload_df("country_coverage", data["country_coverage"])
upload_df("region_coverage", data["region_coverage"])

print("✅ ALL DONE")
