import sys
import requests
import pandas as pd
import time
import os
import pickle

# 💡 安全升级：完美引入 config 里的统一凭证
from config import (
    FEISHU_APP_ID,
    FEISHU_APP_SECRET,
    FEISHU_APP_TOKEN,
    EXCLUDE_TABLES,
    SHEET_CONFIG,
    VERSION_MAPS
)
from data_transformers import (
    transform_quickcheck_adaptive, 
    build_fact_submission
)
from google_uploader import upload_dataframe_to_google_sheet


# =============================================================================
# 📥 1. 飞书数据纯内存、单线程、带重试下载函数
# =============================================================================
def fetch_all_feishu_data():
    """从飞书 API 串行下载所有未被排除的多维表格，并以字典形式返回 {"表名": DataFrame}"""
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    try:
        token_res = requests.post(token_url, json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET})
        token_data = token_res.json()
        if "tenant_access_token" not in token_data:
            print("❌ Failed to acquire Feishu access token:", token_data)
            sys.exit(1)
        token = token_data["tenant_access_token"]
    except Exception as e:
        print(f"❌ Network error when acquiring Feishu token: {e}")
        sys.exit(1)

    headers = {"Authorization": f"Bearer {token}"}
    tables_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables"
    
    try:
        tables_res = requests.get(tables_url, headers=headers)
        tables_data = tables_res.json()
        if tables_data.get("code") != 0:
            print("❌ Failed to fetch tables list:", tables_data.get("msg"))
            sys.exit(1)
        tables = tables_data.get("data", {}).get("items", [])
    except Exception as e:
        print(f"❌ Network error when fetching tables list: {e}")
        sys.exit(1)

    data_dict = {}
    for t in tables:
        t_name = t.get("name")
        t_id = t.get("table_id")
        
        if t_name in EXCLUDE_TABLES:
            print(f"🚫 [Skip] Sub-tab [{t_name}] is in Blacklist.")
            continue
            
        print(f"📥 [Downloading] Sub-tab: [{t_name}] (ID: {t_id})...")
        
        records = []
        page_token = None
        has_more = True
        
        records_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{t_id}/records"
        
        while has_more:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
                
            retry_count = 3
            success = False
            
            for attempt in range(retry_count):
                try:
                    r_res = requests.get(records_url, headers=headers, params=params, timeout=30)
                    r_json = r_res.json()
                    if r_json.get("code") == 0:
                        data_payload = r_json.get("data", {})
                        items = data_payload.get("items", [])
                        for item in items:
                            fields = item.get("fields", {})
                            fields["Record ID"] = item.get("record_id")
                            records.append(fields)
                            
                        has_more = data_payload.get("has_more", False)
                        page_token = data_payload.get("page_token", None)
                        success = True
                        break
                    else:
                        print(f"  ⚠️ Error code {r_json.get('code')}, retrying ({attempt+1}/{retry_count})...")
                        time.sleep(2)
                except Exception as ex:
                    print(f"  ⚠️ Exception occurred: {ex}, retrying ({attempt+1}/{retry_count})...")
                    time.sleep(2)
                    
            if not success:
                print(f"❌ Failed to sync table [{t_name}] entirely after 3 retries.")
                break
                
            time.sleep(0.1)
            
        df = pd.DataFrame(records)
        data_dict[t_name] = df
        print(f"  ✨ Downloaded {len(df)} rows for [{t_name}]")
        
    return data_dict


# =============================================================================
# 🚀 2. 主执行调度引擎（智能环境感知自适应版）
# =============================================================================
def main():
    print("==================================================")
    print("🎬 STARTING SYNC DIRECTION: FEISHU TO GOOGLE SHEETS")
    print("==================================================")
    
    cache_file = "feishu_data_cache.pkl"
    
    # 💡 智能拦截：如果在 GitHub Actions 上运行（能检测到密钥环境变量），强制每次都下载最新数据
    if os.environ.get("GOOGLE_CREDENTIALS"):
        print("☁️ Running on GitHub Actions. Forcing fresh download from Feishu API...")
        feishu_tables = fetch_all_feishu_data()
    else:
        # 💻 本地调试模式：保留本地缓存机制，防止本地测试时频繁下载耗费时间
        if os.path.exists(cache_file):
            print(f"📦 [Local Mode] Found local cache '{cache_file}'. Loading historical data...")
            with open(cache_file, 'rb') as f:
                feishu_tables = pickle.load(f)
        else:
            print("💻 [Local Mode] No local cache found. Initiating full download...")
            feishu_tables = fetch_all_feishu_data()
            print(f"💾 Saving downloaded data to local cache '{cache_file}' for future fast debugging...")
            with open(cache_file, 'wb') as f:
                pickle.dump(feishu_tables, f)

    df_long_list = []
    df_fact_submission_list = []

    print("\n🔮 Transforming sub-tables data...")
    for sheet_name, df_raw in feishu_tables.items():
        if df_raw.empty:
            continue

        config = SHEET_CONFIG.get(sheet_name, {"structure": "simple", "category": "Quick Check"})
        struct_v = config.get("structure", "simple")
        cat_v = config.get("category", "Quick Check")

        # 1. 提炼长表明细数据
        df_long = transform_quickcheck_adaptive(
            df_raw, 
            sheet_name=sheet_name, 
            structure_version=struct_v, 
            category_version=cat_v,
            maps_config=VERSION_MAPS
        )
        if not df_long.empty:
            df_long_list.append(df_long)

        # 2. 提炼提交次数统计事实数据
        df_fact = build_fact_submission(df_raw, form_sheet_name=sheet_name)
        if not df_fact.empty:
            df_fact_submission_list.append(df_fact)

    # 聚合汇总
    if df_long_list:
        df_long_all = pd.concat(df_long_list, ignore_index=True)
    else:
        df_long_all = pd.DataFrame()

    if df_fact_submission_list:
        df_fact_submission_all = pd.concat(df_fact_submission_list, ignore_index=True)
        # 完美细节：因为现在时间是 YYYY/MM/DD 字符串格式，直接 sort_values 同样可以按时间完美排序
        if "SubmissionTime" in df_fact_submission_all.columns:
            df_fact_submission_all = df_fact_submission_all.sort_values(by="SubmissionTime").reset_index(drop=True)
    else:
        df_fact_submission_all = pd.DataFrame()

    print(f"\n📊 Aggregation Completed:")
    print(f"  -> Total records in fact_question_long: {len(df_long_all)} rows.")
    print(f"  -> Total records in fact_submission: {len(df_fact_submission_all)} rows.")

    # =============================================================================
    # 📤 3. 一键覆盖推送到 Google Sheets 云端
    # =============================================================================
    print("\n🚀 Initiating cloud sync via Google Sheets API...")
    
    # 1. 同步长表大盘
    upload_dataframe_to_google_sheet("fact_question_long", df_long_all)
    
    # 2. 同步提交统计明细
    upload_dataframe_to_google_sheet("fact_submission", df_fact_submission_all)

    print("\n==================================================")
    print("🎉 ALL DATA SUCCESSFULLY STREAMED TO GOOGLE SHEETS!")
    print("==================================================")

if __name__ == "__main__":
    main()
