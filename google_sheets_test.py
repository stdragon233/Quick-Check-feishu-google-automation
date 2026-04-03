import gspread
from google.oauth2.service_account import Credentials

# Auth
scope = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
client = gspread.authorize(creds)

# Open sheet
sheet = client.open("Test Automation").sheet1

print("✅ Connected to Google Sheets!")