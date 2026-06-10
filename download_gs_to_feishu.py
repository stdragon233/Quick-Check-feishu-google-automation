import os
import re
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
import time

# 💡 安全升级：直接引入 config 凭证中心和 Google 授权逻辑
from config import (
    FEISHU_APP_ID,
    FEISHU_APP_SECRET,
    GOOGLE_CREDENTIALS_JSON,
    LOCAL_GOOGLE_CONFIG_PATH
)

# ==========================================
# ⚙️ 配置中心（未来切换正式表只需改这里）
# ==========================================
# 1. Google Sheets 配置
GOOGLE_SPREADSHEET_ID = "1mc4JNOVy9dPZEu2Xc-c9duRP6_otwNg2hbnGeZw9wV4"

# 2. 目标飞书 Wiki 链接（当前为你的副本表）
FEISHU_WIKI_URL = "https://xiaopeng.feishu.cn/wiki/JlvgwGUdZiuAHykasMgcZkw8n0g?sheet=c68377"

# 3. 工作表名称精准映射
# 格式：{"Google表名": "飞书表名"}
SHEET_NAME_MAPPING = {
    "fact_submission": "Submission Facts",       # 从 Google 的 fact_submission 写入飞书的 Submission Facts
    "fact_question_long": "fact_question_long"   # 从 Google 的 fact_question_long 写入飞书的 fact_question_long
}

# ==========================================
# 🛠️ 核心逻辑中心（智能自适应安全版）
# ==========================================

def get_google_client_for_download():
    """
    智能获取 Google API 客户端
    优先读取 GitHub 环境变量中的 JSON 字符串，本地测试则读取 secret/gsp-config.json 文件
    """
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # 1. 尝试从环境变量加载 (GitHub Actions 环境)
    if GOOGLE_CREDENTIALS_JSON:
        try:
            print("🔐 Loading Google credentials from GitHub Environment Secrets...")
            creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"⚠️ Failed to parse GOOGLE_CREDENTIALS environment variable: {e}")
            print("🔄 Falling back to local configuration file...")

    # 2. 兜底方案：从本地文件加载 (本地开发环境)
    if os.path.exists(LOCAL_GOOGLE_CONFIG_PATH):
        print(f"📂 Loading Google credentials from local file: {LOCAL_GOOGLE_CONFIG_PATH}")
        try:
            creds = Credentials.from_service_account_info(json.load(open(LOCAL_GOOGLE_CONFIG_PATH)), scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            print(f"❌ Failed to load local Google credentials file: {e}")
            raise e
    else:
        raise FileNotFoundError(f"🛑 找不到 Google 凭证！环境变量和本地路径 [{LOCAL_GOOGLE_CONFIG_PATH}] 均无可落脚点。")


def get_feishu_tenant_access_token():
    """获取飞书多维表格 API Token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_SECRET
    }
    try:
        res = requests.post(url, json=payload)
        res_data = res.json()
        if res_data.get("code") == 0:
            return res_data.get("tenant_access_token")
        else:
            print(f"❌ 飞书 Token 获取失败: {res_data.get('msg')}")
            return None
    except Exception as e:
        print(f"❌ 飞书 Token 请求网络异常: {e}")
        return None

def extract_spreadsheet_token_from_wiki(feishu_token, wiki_token):
    """通过 Wiki Token 置换出多维表格真正的 Spreadsheet Token"""
    url = f"https://open.feishu.cn/open-apis/wiki/v2/nodes/{wiki_token}"
    headers = {"Authorization": f"Bearer {feishu_token}"}
    try:
        res = requests.get(url, headers=headers)
        res_data = res.json()
        if res_data.get("code") == 0:
            node_info = res_data.get("data", {}).get("node", {})
            if node_info.get("obj_type") == "bitable":
                return node_info.get("obj_token")
            else:
                print(f"❌ 该 Wiki 节点不是多维表格(Bitable)，实际类型为: {node_info.get('obj_type')}")
                return None
        else:
            print(f"❌ Wiki 节点解构失败: {res_data.get('msg')}")
            return None
    except Exception as e:
        print(f"❌ Wiki 节点转换网络异常: {e}")
        return None

def get_feishu_sheet_id_by_name(feishu_token, spreadsheet_token, sheet_name):
    """根据子表名称获取飞书的子表 ID (表格条目中形如 tbl_xxxxxx)"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{spreadsheet_token}/tables"
    headers = {"Authorization": f"Bearer {feishu_token}"}
    try:
        res = requests.get(url, headers=headers)
        res_data = res.json()
        if res_data.get("code") == 0:
            tables = res_data.get("data", {}).get("items", [])
            for t in tables:
                if t.get("name") == sheet_name:
                    return t.get("table_id")
            print(f"⚠️ 飞书多维表格中未找到名为 [{sheet_name}] 的子表。")
            return None
        else:
            print(f"❌ 获取飞书子表列表失败: {res_data.get('msg')}")
            return None
    except Exception as e:
        print(f"❌ 获取子表 ID 网络异常: {e}")
        return None

def clear_feishu_table_records_batch(feishu_token, spreadsheet_token, table_id):
    """批量清空飞书对应子表的所有历史留存数据"""
    headers = {"Authorization": f"Bearer {feishu_token}"}
    base_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{spreadsheet_token}/tables/{table_id}/records"
    
    print(f"🧹 正在扫描并清理飞书表 [{table_id}] 的历史残余数据...")
    while True:
        try:
            res = requests.get(base_url, headers=headers, params={"page_size": 100})
            res_data = res.json()
            if res_data.get("code") != 0:
                print(f"❌ 读取飞书历史记录失败: {res_data.get('msg')}")
                break
                
            items = res_data.get("data", {}).get("items", [])
            if not items:
                break
                
            record_ids = [item.get("record_id") for item in items]
            delete_url = f"{base_url}/batch_delete"
            del_res = requests.post(delete_url, headers=headers, json={"record_ids": record_ids})
            del_data = del_res.json()
            
            if del_data.get("code") == 0:
                print(f"  🗑️ 成功批量擦除 {len(record_ids)} 条历史残余数据。")
            else:
                print(f"  ⚠️ 批量删除异常: {del_data.get('msg')}")
                break
                
            time.sleep(0.2)
        except Exception as e:
            print(f"❌ 清理历史数据时发生网络突发异常: {e}")
            break

def write_data_to_feishu_sheet(feishu_token, spreadsheet_token, sheet_name, df):
    """将清洗后的 Pandas DataFrame 批量高规灌注回对应的飞书多维表格子表"""
    table_id = get_feishu_sheet_id_by_name(feishu_token, spreadsheet_token, sheet_name)
    if not table_id:
        print(f"❌ 无法获取子表 [{sheet_name}] 的 table_id，跳过此表灌注。")
        return
        
    clear_feishu_table_records_batch(feishu_token, spreadsheet_token, table_id)
    
    # 清洗特殊空值
    df_clean = df.copy()
    # 将包含空字符串、NaN、None 的地方安全兼容
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    headers = {"Authorization": f"Bearer {feishu_token}"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{spreadsheet_token}/tables/{table_id}/records/batch_create"
    
    records = []
    for _, row in df_clean.iterrows():
        fields = {}
        for col in df_clean.columns:
            val = row[col]
            if val == "" or val is None or (isinstance(val, float) and pd.isna(val)):
                continue
            fields[col] = val
        records.append({"fields": fields})
        
    print(f"🚀 准备向飞书子表 [{sheet_name}] 批量灌注 {len(records)} 条全新数据...")
    
    # 飞书批量写入接口单次最大支持 500 条
    batch_size = 400
    for i in range(0, len(records), batch_size):
        chunk = records[i:i+batch_size]
        max_retries = 3
        success = False
        
        for attempt in range(1, max_retries + 1):
            try:
                res = requests.post(url, headers=headers, json={"records": chunk})
                res_data = res.json()
                if res_data.get("code") == 0:
                    print(f"  ⚡ [{sheet_name}] 成功灌注第 {i+1} ~ {min(i+batch_size, len(records))} 条记录")
                    success = True
                    break
                else:
                    print(f"  ⚠️ 灌注失败重试中 ({attempt}/{max_retries}): {res_data.get('msg')}")
                    time.sleep(2 * attempt)
            except Exception as e:
                print(f"  ⚠️ 灌注请求网络闪退 ({attempt}/{max_retries}): {e}")
                time.sleep(2 * attempt)
                
        if not success:
            print(f"❌ [严重错误] 子表 [{sheet_name}] 在分批写入第 {i+1} 条数据时重试耗尽，灌注被迫断开。")
            break
            
        time.sleep(0.2)

def main():
    print("==================================================")
    print("🎬 STARTING SYNC DIRECTION: GOOGLE SHEETS TO FEISHU")
    print("==================================================")
    
    # 1. 飞书通用鉴权校验
    feishu_token = get_feishu_tenant_access_token()
    if not feishu_token:
        print("🛑 程序终止：无法获取有效的飞书 Tenant Access Token，请检查 config.py 配置")
        return
        
    wiki_match = re.search(r'/wiki/([a-zA-Z0-9]+)', FEISHU_WIKI_URL)
    if not wiki_match:
        print("🛑 程序终止：无法从 FEISHU_WIKI_URL 中解析出有效的 Wiki Token，请确认链接格式")
        return
    wiki_token = wiki_match.group(1)
    
    spreadsheet_token = extract_spreadsheet_token_from_wiki(feishu_token, wiki_token)
    if not spreadsheet_token:
        print("🛑 程序终止：无法从 Wiki 节点置换出多维表格的 obj_token")
        return
    print(f"✅ 成功从 Wiki 节点中置换出目标多维表格 Token: {spreadsheet_token}")

    # 2. Google Sheets 通用鉴权智能校验
    try:
        gc = get_google_client_for_download()
        sh = gc.open_by_key(GOOGLE_SPREADSHEET_ID)
        print("✅ Google Sheets 授权并打开大盘表成功")
    except Exception as e:
        print(f"🛑 程序终止：授权失败: {e}")
        return
    
    print("\n📥 Fetching from Google and streaming back to Feishu...")
    success_count = 0
    fail_count = 0
    
    for gs_name, feishu_name in SHEET_NAME_MAPPING.items():
        try:
            print(f"\n--- 正在处理: Google [{gs_name}] -> 飞书 [{feishu_name}] ---")
            ws = sh.worksheet(gs_name)
            data = ws.get_all_records()
            
            if not data:
                print(f"⚠️ 警告：Google 工作表 [{gs_name}] 中没有数据，跳过同步。")
                continue
                
            df = pd.DataFrame(data)
            print(f"✅ 成功从 Google 下载数据，共抓取到 {len(df)} 行，{len(df.columns)} 列。")
            
            write_data_to_feishu_sheet(feishu_token, spreadsheet_token, feishu_name, df)
            success_count += 1
            
        except Exception as e:
            print(f"❌ 处理映射 [{gs_name} -> {feishu_name}] 时遭遇未知阻碍: {e}")
            import traceback
            traceback.print_exc()
            fail_count += 1
            
    print("\n==================================================")
    print(f"🎉 倒灌任务同步完毕！成功: {success_count} 张子表, 失败: {fail_count} 张子表")
    print("==================================================")

if __name__ == "__main__":
    main()
