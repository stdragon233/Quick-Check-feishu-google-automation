import sys
import requests
import pandas as pd
import time
import os
import pickle

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
    build_fact_submission,
    enrich_store_info
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
    print("✅ Feishu Token acquired successfully.")

    list_tables_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables"
    tables_res = requests.get(list_tables_url, headers=headers)
    tables_data = tables_res.json()

    if tables_data.get("code") != 0:
        print(f"❌ Failed to fetch Feishu tables list: {tables_data.get('msg')}")
        sys.exit(1)

    all_fetched_tables = tables_data.get("data", {}).get("items", [])
    
    tables_to_download = []
    for table in all_fetched_tables:
        if table["name"] in EXCLUDE_TABLES or table["table_id"] in EXCLUDE_TABLES:
            print(f"⏭️ Skipping excluded table: {table['name']}")
            continue
        tables_to_download.append(table)

    print(f"📋 Total tables in Feishu: {len(all_fetched_tables)}")
    print(f"🔒 Sequentially downloading {len(tables_to_download)} tables to memory (Stable Mode)...")

    all_dataframes = {}

    for index, table in enumerate(tables_to_download, 1):
        table_id = table["table_id"]
        sheet_name = table["name"]
        
        records_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{table_id}/records"
        
        all_records = []
        has_more = True
        page_token = ""
        
        while has_more:
            params = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
                
            max_retries = 3
            success = False
            
            for attempt in range(1, max_retries + 1):
                try:
                    res = requests.get(records_url, headers=headers, params=params)
                    data = res.json()
                    
                    if data.get("code") == 1254003 or "Data not ready" in data.get("msg", ""):
                        wait_time = attempt * 2.0
                        print(f"  ⏳ [{sheet_name}] Server busy. Retrying in {wait_time}s... ({attempt}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    elif data.get("code") != 0:
                        print(f"  ⚠️ Feishu Error reply in '{sheet_name}': {data.get('msg')}")
                        break
                    else:
                        success = True
                        break
                except Exception as e:
                    print(f"  ⚠️ Network glitch in '{sheet_name}': {e}. Retrying in 2s...")
                    time.sleep(2)
                    
            if not success:
                print(f"\n❌ [CRITICAL ERROR] Failed to download '{sheet_name}' after {max_retries} attempts.")
                sys.exit(1)
                
            items = data.get("data", {}).get("items", [])
            all_records.extend(items)
            has_more = data.get("data", {}).get("has_more", False)
            page_token = data.get("data", {}).get("page_token", "")
            
        if all_records:
            df = pd.DataFrame([item["fields"] for item in all_records])
        else:
            df = pd.DataFrame()
            
        all_dataframes[sheet_name] = df
        print(f"  [{index}/{len(tables_to_download)}] ✅ Successfully loaded: '{sheet_name}' ({len(df)} rows)")
        time.sleep(0.1)

    print("\n🎉 All target data successfully loaded into memory python dictionary!")
    return all_dataframes


# =============================================================================
# 🚀 2. 主调度流程 (The Execution Grandmaster)
# =============================================================================
def main():
    print("==================================================")
    print("🎬 STARTING AUTOMATION: FEISHU TO GOOGLE SHEETS")
    print("==================================================")
    
    # 直接下载最新数据（GitHub Actions 和本地都统一行为）
    print("🔍 Fetching fresh data from Feishu...")
    feishu_data = fetch_all_feishu_data()
    
    # =========================================================================
    # 🆕 分离门店明细表（如果存在）
    # =========================================================================
    store_master_df = None
    if "门店明细（引用）" in feishu_data:
        store_master_df = feishu_data.pop("门店明细（引用）")
        print(f"📋 已分离门店明细表，共 {len(store_master_df)} 行")
    else:
        print("⚠️ 未找到门店明细表，将跳过地理位置信息修正")
    
    dfs_long_pool = []
    dfs_submission_pool = []

    print("\n⚙️ Processing and transforming data...")
    for sheet_name, df_raw in feishu_data.items():
        # 第一层保险：跳过空表
        if df_raw.empty or len(df_raw) == 0:
            print(f"  ⏩ Passed empty sheet: [{sheet_name}]")
            continue
        
        # 第二层保险：只处理包含 "check" 的表（不区分大小写）
        if "check" not in sheet_name.lower():
            print(f"  ⏭️ Skipped non-check sheet: [{sheet_name}] (no 'check' in name)")
            continue

        try:
            # 💡 从你的 SHEET_CONFIG 里面动态获取每个国家表专属的 structure 和 category 设定
            # 如果没配，默认作为 "simple" 和 "Quick Check" 处理
            cfg = SHEET_CONFIG.get(sheet_name, {"structure": "simple", "category": "Quick Check"})
            struct_ver = cfg.get("structure", "simple")
            cat_ver = cfg.get("category", "Quick Check")

            # 1. 原汁原味宽转长表转化 (动态传入国家表对应的配置)
            df_long = transform_quickcheck_adaptive(
                df_raw, 
                sheet_name=sheet_name, 
                structure_version=struct_ver, 
                category_version=cat_ver,
                maps_config=VERSION_MAPS
            )
            if not df_long.empty:
                dfs_long_pool.append(df_long)
                
            # 2. 原汁原味事实提交明细表提取
            df_submission = build_fact_submission(df_raw, form_sheet_name=sheet_name)
            if not df_submission.empty:
                dfs_submission_pool.append(df_submission)
                
            print(f"  ✨ Successfully transformed sheet: [{sheet_name}]")
        except Exception as e:
            print(f"  ❌ Error encountered while analyzing [{sheet_name}]: {e}")
            import traceback
            traceback.print_exc()
            print("🛑 Program terminates to avoid partial upload.")
            sys.exit(1)

    # 聚合总表
    if dfs_long_pool:
        df_long_all = pd.concat(dfs_long_pool, ignore_index=True)
    else:
        df_long_all = pd.DataFrame()

    if dfs_submission_pool:
        df_fact_submission_all = pd.concat(dfs_submission_pool, ignore_index=True)
        
        if "SubmissionTime" in df_fact_submission_all.columns:
            df_fact_submission_all = df_fact_submission_all.sort_values(by="SubmissionTime").reset_index(drop=True)
    else:
        df_fact_submission_all = pd.DataFrame()

    print(f"\n📊 Aggregation Completed:")
    print(f"  -> Total records in fact_question_long: {len(df_long_all)} rows.")
    print(f"  -> Total records in fact_submission: {len(df_fact_submission_all)} rows.")

    # =========================================================================
    # 🆕 使用门店明细表修正地理位置信息（方案B：转换后修正）
    # =========================================================================
    if store_master_df is not None and not store_master_df.empty:
        print("\n📍 Enriching store location information...")
        df_long_all, df_fact_submission_all = enrich_store_info(
            df_long_all,
            df_fact_submission_all,
            store_master_df
        )
    else:
        print("\n⚠️ 跳过门店地理位置信息修正（无门店明细表）")

    # =========================================================================
    # 📤 3. 一键覆盖推送到 Google Sheets 云端
    # =========================================================================
    print("\n🚀 Initiating cloud sync via Google Sheets API...")
    
    # 1. 同步长表大盘
    upload_dataframe_to_google_sheet("fact_question_long", df_long_all)
    
    # 2. 同步提交统计明细
    upload_dataframe_to_google_sheet("fact_submission", df_fact_submission_all)
    
    print("\n==================================================")
    print("🎉 ALL SYSTEM TASKS SUCCESSFULLY EXECUTED!")
    print("==================================================")

if __name__ == "__main__":
    main()
