import os
import re
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
import time
from datetime import datetime

# 💡 安全升级：直接引入 config 凭证中心和 Google 授权逻辑
from config import (
    FEISHU_APP_ID,
    FEISHU_APP_SECRET,
    GOOGLE_CREDENTIALS_JSON,
    LOCAL_GOOGLE_CONFIG_PATH
)

# ==========================================
# ⚙️ 配置中心
# ==========================================
# 1. Google Sheets 配置
GOOGLE_SPREADSHEET_ID = "1mc4JNOVy9dPZEu2Xc-c9duRP6_otwNg2hbnGeZw9wV4"

# ==========================================
# 📌 目标飞书链接配置（二选一，注释/取消注释即可切换）
# ==========================================

# 【选项 A】原版正式表（独立电子表格 Sheets 链接）
FEISHU_SHEETS_URL = "https://xiaopeng.feishu.cn/sheets/JfSiseGIehy9sbtWYJocZwdrn7e?sheet=FJlyPz"
FEISHU_SPREADSHEET_TOKEN = "JfSiseGIehy9sbtWYJocZwdrn7e"

# 【选项 B】测试副本表（Wiki 链接，保留用于测试切换）
# FEISHU_WIKI_URL = "https://xiaopeng.feishu.cn/wiki/JlvgwGUdZiuAHykasMgcZkw8n0g?sheet=c68377"

# ==========================================
# 🧠 自动模式选择
# ==========================================
USE_DIRECT_SHEETS_MODE = 'FEISHU_SPREADSHEET_TOKEN' in dir() and FEISHU_SPREADSHEET_TOKEN
USE_WIKI_MODE = not USE_DIRECT_SHEETS_MODE and 'FEISHU_WIKI_URL' in dir() and FEISHU_WIKI_URL

# 工作表名称精准映射
SHEET_NAME_MAPPING = {
    "fact_submission": "Submission Facts",       
    "fact_question_long": "fact_question_long"   
}

# ==========================================
# 🛠️ 核心逻辑中心
# ==========================================

def get_google_client_for_download():
    """
    智能获取 Google API 客户端
    优先读取 GitHub 环境变量中的 JSON 字符串，本地测试则读取 secret/gsp-config.json 文件
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    if GOOGLE_CREDENTIALS_JSON:
        try:
            print("🔐 Loading Google credentials from GitHub Environment Secrets...")
            creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"⚠️ Failed to parse GOOGLE_CREDENTIALS: {e}")
            print("🔄 Falling back to local file...")

    if os.path.exists(LOCAL_GOOGLE_CONFIG_PATH):
        print(f"📂 Loading Google credentials from local file: {LOCAL_GOOGLE_CONFIG_PATH}")
        try:
            with open(LOCAL_GOOGLE_CONFIG_PATH, 'r') as f:
                creds_dict = json.load(f)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"❌ Failed to load local credentials: {e}")
            raise e
    else:
        raise FileNotFoundError(f"🛑 找不到 Google 凭证！")

def get_feishu_tenant_access_token():
    """获取飞书 Tenant Access Token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    try:
        res = requests.post(url, json=payload)
        res_data = res.json()
        if res_data.get("code") == 0:
            print("✅ 成功获取飞书 Tenant Access Token")
            return res_data.get("tenant_access_token")
        else:
            print(f"❌ 飞书 Token 获取失败: {res_data.get('msg')}")
            return None
    except Exception as e:
        print(f"❌ 飞书 Token 请求异常: {e}")
        return None

def extract_wiki_token(url):
    """从飞书 Wiki 链接中提取 Wiki Token"""
    match = re.search(r'/wiki/([a-zA-Z0-9]+)', url)
    return match.group(1) if match else None

def convert_wiki_to_spreadsheet_token(tenant_token, wiki_token):
    """将 Wiki Token 转换为 Spreadsheet Token"""
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    try:
        res = requests.get(url, headers=headers).json()
        if res.get("code") == 0:
            node = res.get("data", {}).get("node", {})
            if node.get("obj_type") == "sheet":
                spreadsheet_token = node.get("obj_token")
                print(f"🔓 成功解密 Wiki 节点！Spreadsheet Token: {spreadsheet_token}")
                return spreadsheet_token
        print(f"❌ Wiki 转换失败: {res.get('msg')}")
        return None
    except:
        return None

def get_sheet_id_by_name(tenant_token, spreadsheet_token, sheet_name):
    """根据工作表名称获取 sheet_id 和当前行数"""
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    try:
        res = requests.get(url, headers=headers).json()
        if res.get("code") == 0:
            sheets = res.get("data", {}).get("sheets", [])
            for sheet in sheets:
                if sheet.get("title") == sheet_name:
                    sheet_id = sheet.get("sheetId")
                    row_count = sheet.get("rowCount", 0)
                    print(f"🔍 探测工作表 [{sheet_name}] -> ID: {sheet_id}, 行数: {row_count}")
                    return sheet_id, row_count
        print(f"❌ 未找到工作表 [{sheet_name}]")
        return None, None
    except:
        return None, None

def add_rows_to_sheet(tenant_token, spreadsheet_token, sheet_id, current_rows, needed_rows):
    """如果数据行数超过当前工作表行数，自动增加行"""
    if needed_rows <= current_rows:
        return True
    
    rows_to_add = needed_rows - current_rows
    print(f"📏 需要增加 {rows_to_add} 行（当前: {current_rows}，需要: {needed_rows}）")
    
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    payload = {"dimension": {"sheetId": sheet_id, "majorDimension": "ROWS", "length": rows_to_add}}
    try:
        res = requests.post(url, json=payload, headers=headers).json()
        if res.get("code") == 0:
            print(f"▲ 成功扩容 {rows_to_add} 行")
            return True
        return False
    except:
        return False

def get_column_letter(col_num):
    """将数字列号转换为 Excel 字母列号"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = chr(col_num % 26 + 65) + result
        col_num //= 26
    return result

def datetime_to_feishu_serial(dt_str):
    """将时间转换为飞书电子表格的序列号"""
    try:
        dt = pd.to_datetime(dt_str)
        base_date = datetime(1899, 12, 30)
        delta = dt - base_date
        return delta.total_seconds() / 86400.0
    except:
        return dt_str

def force_apply_post_styles(tenant_token, spreadsheet_token, sheet_id, sheet_name, total_rows):
    """强制刷整列格式属性"""
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style"
    headers = {"Authorization": f"Bearer {tenant_token}"}
    
    time_col = "B" if sheet_name == "Submission Facts" else "C"
    print(f"🎨 锁定 [{sheet_name}] 的 {time_col} 列格式...")
    
    time_payload = {
        "appendStyle": {
            "range": f"{sheet_id}!{time_col}:{time_col}",
            "style": {"formatter": "yyyy/MM/dd HH:mm:ss"}
        }
    }
    try:
        requests.put(url, json=time_payload, headers=headers)
        print(f"   ✅ {time_col} 列日期格式已锁定")
    except Exception as e:
        print(f"   ⚠️ {time_col} 列格式失败: {e}")
        
    if sheet_name == "Submission Facts":
        rate_cols = ["J", "L", "P", "T", "X"]
        for r_col in rate_cols:
            rate_payload = {
                "appendStyle": {
                    "range": f"{sheet_id}!{r_col}:{r_col}",
                    "style": {"formatter": "0.00%"}
                }
            }
            try:
                requests.put(url, json=rate_payload, headers=headers)
                print(f"   ✅ {r_col} 列百分比格式已锁定")
            except:
                pass

def write_data_to_feishu_sheet(tenant_token, spreadsheet_token, sheet_name, df):
    """全量覆盖写入飞书指定的子工作表"""
    print(f"⚙️ 同步到飞书子表 [{sheet_name}]...")
    
    sheet_id, current_rows = get_sheet_id_by_name(tenant_token, spreadsheet_token, sheet_name)
    if not sheet_id:
        print(f"❌ 无法获取工作表 [{sheet_name}] 的 ID")
        return
    
    # 清洗转换数据
    grid_values = [df.columns.tolist()]
    date_pattern = re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}')

    for row_idx, row in enumerate(df.values):
        clean_row = []
        for col_idx, cell in enumerate(row):
            if pd.isna(cell) or cell is None or str(cell).strip() in ["", "nan", "NaN", "None", "NaT"]:
                clean_row.append(None)
                continue
            
            cell_str = str(cell).strip()
            
            if sheet_name == "fact_question_long" and col_idx == 2 and row_idx < 5:
                print(f"🔬 [探针] C列第 {row_idx+2} 行: '{cell_str}'")
            
            is_date = False
            if date_pattern.match(cell_str):
                is_date = True
            elif sheet_name == "fact_question_long" and col_idx == 2:
                try:
                    pd.to_datetime(cell_str)
                    is_date = True
                except:
                    pass
            
            if is_date:
                clean_row.append(datetime_to_feishu_serial(cell_str))
                continue
            
            try:
                num_val = pd.to_numeric(cell_str)
                clean_row.append(int(num_val) if int(num_val) == num_val else float(num_val))
                continue
            except:
                pass
            
            clean_row.append(cell_str)
        grid_values.append(clean_row)
        
    data_rows = len(grid_values)
    end_col_letter = get_column_letter(len(df.columns))
    headers = {"Authorization": f"Bearer {tenant_token}"}

    # 清除旧数据
    print(f"🧹 清空旧数据...")
    clear_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_clear"
    clear_payload = {"range": f"{sheet_id}!A1:{end_col_letter}{max(current_rows, 2000)}"}
    requests.post(clear_url, json=clear_payload, headers=headers)
    
    # 检查并增加行数
    if not add_rows_to_sheet(tenant_token, spreadsheet_token, sheet_id, current_rows, data_rows):
        print(f"❌ 扩容失败")
        return
    
    # 分批写入
    write_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update"
    chunk_size = 2000
    print(f"🚀 分批写入（每批 {chunk_size} 行）...")
    
    for i in range(0, data_rows, chunk_size):
        chunk = grid_values[i:i+chunk_size]
        start_row = i + 1
        end_row = start_row + len(chunk) - 1
        batch_range = f"{sheet_id}!A{start_row}:{end_col_letter}{end_row}"
        
        write_payload = {
            "valueInputOption": "PARSE_VALUES",
            "valueRanges": [{"range": batch_range, "values": chunk}]
        }
        
        try:
            res = requests.post(write_url, json=write_payload, headers=headers).json()
            if res.get("code") != 0:
                print(f"❌ 写入失败: {res.get('msg')}")
                return
        except Exception as e:
            print(f"❌ 写入异常: {e}")
            return
            
    print(f"✨ 同步完成，共 {len(grid_values)-1} 行")
    force_apply_post_styles(tenant_token, spreadsheet_token, sheet_id, sheet_name, data_rows)

def main():
    print("==================================================")
    print("🔄 STARTING REVERSE SYNC: GOOGLE SHEETS TO FEISHU")
    print("==================================================")
    
    # 1. 飞书鉴权
    print("\n🔑 Authenticating with Feishu API...")
    feishu_token = get_feishu_tenant_access_token()
    if not feishu_token:
        print("🛑 程序终止：飞书鉴权失败")
        return
    
    # 2. 获取 Spreadsheet Token
    spreadsheet_token = None
    
    if USE_DIRECT_SHEETS_MODE:
        print("📌 使用模式：直接 Sheets 链接（原版正式表）")
        spreadsheet_token = FEISHU_SPREADSHEET_TOKEN
        print(f"✅ Spreadsheet Token: {spreadsheet_token}")
    elif USE_WIKI_MODE:
        print("📌 使用模式：Wiki 链接（测试副本）")
        wiki_token = extract_wiki_token(FEISHU_WIKI_URL)
        if not wiki_token:
            print("🛑 程序终止：无法解析 Wiki 链接")
            return
        spreadsheet_token = convert_wiki_to_spreadsheet_token(feishu_token, wiki_token)
    else:
        print("🛑 程序终止：未配置任何有效的飞书链接")
        print("💡 请在代码顶部的配置中心启用 FEISHU_SPREADSHEET_TOKEN 或 FEISHU_WIKI_URL")
        return
    
    if not spreadsheet_token:
        print("🛑 程序终止：无法获取有效的 Spreadsheet Token")
        return

    # 3. Google Sheets 授权
    print("\n🔑 Authorizing Google Sheets Client...")
    try:
        gc = get_google_client_for_download()
        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        print("✅ Google Sheets 授权成功")
    except Exception as e:
        print(f"🛑 程序终止：授权失败: {e}")
        return
    
    # 4. 同步数据
    print("\n📥 Fetching from Google and streaming back to Feishu...")
    success_count = 0
    fail_count = 0
    
    for gs_name, feishu_name in SHEET_NAME_MAPPING.items():
        try:
            print(f"\n--- 正在处理: Google [{gs_name}] -> 飞书 [{feishu_name}] ---")
            ws = sh.worksheet(gs_name)
            data = ws.get_all_records()
            
            if not data:
                print(f"⚠️ 警告：Google 工作表 [{gs_name}] 中没有数据，跳过同步")
                continue
                
            df = pd.DataFrame(data)
            print(f"✅ 成功从 Google 下载数据，共 {len(df)} 行，{len(df.columns)} 列")
            
            write_data_to_feishu_sheet(feishu_token, spreadsheet_token, feishu_name, df)
            success_count += 1
            
        except Exception as e:
            print(f"❌ 处理映射 [{gs_name} -> {feishu_name}] 时遇到错误: {e}")
            import traceback
            traceback.print_exc()
            fail_count += 1
    
    print("\n==================================================")
    print(f"📊 同步完成统计：成功 {success_count} 个，失败 {fail_count} 个")
    print("==================================================")

if __name__ == "__main__":
    main()
