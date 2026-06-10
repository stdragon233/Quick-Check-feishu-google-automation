import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from config import GOOGLE_CREDENTIALS_JSON, LOCAL_GOOGLE_CONFIG_PATH, TARGET_GOOGLE_SHEET_NAME

def get_google_client():
    """
    智能获取 Google API 客户端
    优先读取 GitHub 环境变量中的 JSON 字符串，本地测试则读取 secret/gsp-config.json 文件
    """
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # 1. 尝试从环境变量加载 (GitHub Actions 环境)
    if GOOGLE_CREDENTIALS_JSON:
        try:
            print("🔐 Loading Google credentials from GitHub Environment Secrets...")
            creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"⚠️ Failed to parse GOOGLE_CREDENTIALS environment variable: {e}")
            print("🔄 Falling back to local configuration file...")

    # 2. 兜底方案：从本地文件加载 (本地开发环境)
    if os.path.exists(LOCAL_GOOGLE_CONFIG_PATH):
        print(f"📂 Loading Google credentials from local file: {LOCAL_GOOGLE_CONFIG_PATH}")
        try:
            creds = Credentials.from_service_account_info(json.load(open(LOCAL_GOOGLE_CONFIG_PATH)), scopes=scope)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"❌ Failed to load local credentials file: {e}")
            raise e
    else:
        raise FileNotFoundError(
            "❌ Google credentials not found! Neither GitHub Secret environment variable "
            f"nor local file at '{LOCAL_GOOGLE_CONFIG_PATH}' is available."
        )


def upload_dataframe_to_google_sheet(sheet_name, df):
    """
    将一个 Pandas DataFrame 全量覆盖写入到指定的 Google Sheet 工作表中
    """
    # 1. 连接 Google API 并打开主电子表格
    client = get_google_client()
    try:
        # 这里动态读取 config.py 里的电子表格名称
        spreadsheet = client.open(TARGET_GOOGLE_SHEET_NAME)
    except Exception as e:
        print(f"❌ Failed to open Google Spreadsheet '{TARGET_GOOGLE_SHEET_NAME}': {e}")
        print("💡 Please double check if the sheet name is correct and shared with your Service Account Email.")
        raise e

    print(f"📤 Preparing to upload data to Tab: [{sheet_name}], total rows: {len(df)}")

    # 2. 获取或创建对应的工作表 (Tab)
    try:
        # 如果工作表已经存在，直接获取并清空历史残余数据
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        print(f"🧹 Tab [{sheet_name}] already exists. Cleared historical data.")
    except gspread.exceptions.WorksheetNotFound:
        # 如果不存在该工作表，自动新建一个
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
        print(f"✨ Created a new Tab [{sheet_name}] in Google Sheets.")

    # 3. 如果 DataFrame 完全为空，写入表头后即可直接返回
    if df.empty:
        print(f"⚠️ DataFrame for [{sheet_name}] is empty. No data rows to upload.")
        return

    # 4. 数据预处理安全网：
    # 转换为字符串和处理空值，防止 Timestamp、NaN 或 None 导致 Google API 报 JSON 序列化错误
    df_clean = df.fillna("").astype(str)

    # 5. 转化为 Google API 认识的二维列表格式: [ [表头1, 表头2], [行1列1, 行1列2], ... ]
    data_to_upload = [df_clean.columns.values.tolist()] + df_clean.values.tolist()

    # 6. 一次性整块打包推送更新 (从 A1 单元格开始填满)
    try:
        worksheet.update("A1", data_to_upload, value_input_option="USER_ENTERED")
        print(f"⚡ Successfully uploaded {len(df)} rows to [{sheet_name}]!")
    except Exception as e:
        print(f"❌ Failed to update cells in Google Sheet [{sheet_name}]: {e}")
        raise e
