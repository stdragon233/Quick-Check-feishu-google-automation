import gspread
from google.oauth2.service_account import Credentials
import os
import json

# Auth
scope = ["https://www.googleapis.com/auth/spreadsheets"]
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

# Open sheet (use your sheet ID)
SHEET_ID = "1mc4JNOVy9dPZEu2Xc-c9duRP6_otwNg2hbnGeZw9wV4"
sheet = client.open_by_key(SHEET_ID).sheet1

print("✅ Connected to Google Sheets!")
