import sys
import requests
import pandas as pd
import time
from config import FEISHU_APP_ID, FEISHU_APP_SECRET, FEISHU_APP_TOKEN, EXCLUDE_TABLES
from data_transformers import (
    transform_quickcheck_adaptive, 
    build_fact_submission, 
    build_dim_store_coverage,
    build_country_coverage,
    build_region_coverage
)
from google_uploader import upload_dataframe_to_google_sheet

# =============================================================================
# 📥 1. 飞书数据纯内存、单线程、带重试下载函数
# =============================================================================
def fetch_all_feishu_data():
    """从飞书 API 串行下载所有未被排除的多维表格，并以字典形式返回 {"表名": DataFrame}"""
    # 获取 Tenant Access Token
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

    # 获取所有表格列表
    list_tables_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables"
    tables_res = requests.get(list_tables_url, headers=headers)
    tables_data = tables_res.json()

    if tables_data.get("code") != 0:
        print(f"❌ Failed to fetch Feishu tables list: {tables_data.get('msg')}")
        sys.exit(1)

    all_fetched_tables = tables_data.get("data", {}).get("items", [])
    
    # 过滤黑名单
    tables_to_download = []
    for table in all_fetched_tables:
        if table["name"] in EXCLUDE_TABLES or table["table_id"] in EXCLUDE_TABLES:
            continue
        tables_to_download.append(table)

    print(f"📋 Total tables in Feishu: {len(all_fetched_tables)}")
    print(f"🔒 Sequentially downloading {len(tables_to_download)} tables to memory (Stable Mode)...")

    all_dataframes = {}

    # 开始串行下载
    for index, table in enumerate(tables_to_download, 1):
        table_id = table["table_id"]
        sheet_name = table["name"]
        
        records_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{table_id}/records"
        
        all_records = []
        has_more = True
        page_token = ""
        
        while has_more:
            params = {"page_size": 500}  # 干净纯净的数据请求，不加主动强算参数
            if page_token:
                params["page_token"] = page_token
                
            max_retries = 3
            success = False
            
            for attempt in range(1, max_retries + 1):
                try:
                    res = requests.get(records_url, headers=headers, params=params)
                    data = res.json()
                    
                    # 遇到飞书服务器繁忙或网关限流的提示
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
                    
            # 🌋 熔断防线：下载失败直接硬停机，宁可今天不更新，也绝对不向下游传递残缺数据
            if not success:
                print(f"\n❌ [CRITICAL ERROR] Failed to download '{sheet_name}' after {max_retries} attempts.")
                print("🛑 Process halted to protect destination cloud sheets from corruption.")
                sys.exit(1)
                
            items = data.get("data", {}).get("items", [])
            all_records.extend(items)
            
            has_more = data.get("data", {}).get("has_more", False)
            page_token = data.get("data", {}).get("page_token", "")
            
        # 组装为 DataFrame
        if all_records:
            df = pd.DataFrame([item["fields"] for item in all_records])
        else:
            df = pd.DataFrame()
            
        all_dataframes[sheet_name] = df
        print(f"  [{index}/{len(tables_to_download)}] ✅ Successfully loaded: '{sheet_name}' ({len(df)} rows)")
        time.sleep(0.1)  # 给飞书网关喘息的体贴空档

    print("\n🎉 All target data successfully loaded into memory python dictionary!")
    return all_dataframes


# =============================================================================
# 🚀 2. 主调度流程 (The Execution Grandmaster)
# =============================================================================
def main():
    print("==================================================")
    print("🎬 STARTING AUTOMATION: FEISHU TO GOOGLE SHEETS")
    print("==================================================")
    
    # Step 2.1: 下载全部飞书数据
    feishu_data = fetch_all_feishu_data()
    
    # 提取门店主表数据（门店明细（引用））
    # 它是我们后续计算覆盖率大盘的核心母表
    store_master_sheet = "门店明细（引用）"
    if store_master_sheet in feishu_data:
        df_store_master = feishu_data[store_master_sheet]
        print(f"\n🏬 Master Store Registry loaded: {len(df_store_master)} rows.")
    else:
        print(f"\n⚠️ Master sheet '{store_master_sheet}' not found in downloaded data!")
        print("Coverage calculations might be inaccurate or skipped.")
        df_store_master = pd.DataFrame()

    # Step 2.2: 循环清洗和内存聚合
    dfs_long_pool = []
    dfs_submission_pool = []

    print("\n⚙️ Processing and transforming data...")
    for sheet_name, df_raw in feishu_data.items():
        # 如果是门店主表本身，不参与横转纵的问卷清洗，安全跳过
        if sheet_name == store_master_sheet:
            continue
            
        # 🩹 空表安全补丁：如果当前国家或表单行数为0，不参与计算
        if df_raw.empty or len(df_raw) == 0:
            print(f"  ⏩ Passed empty sheet: [{sheet_name}]")
            continue

        try:
            # 1. 宽表转长表转化 (带自动公式注入)
            df_long = transform_quickcheck_adaptive(df_raw, sheet_name)
            if not df_long.empty:
                dfs_long_pool.append(df_long)
                
            # 2. 事实提交明细表提取
            df_submission = build_fact_submission(df_raw, sheet_name)
            if not df_submission.empty:
                dfs_submission_pool.append(df_submission)
                
            print(f"  ✨ Successfully transformed sheet: [{sheet_name}]")
        except Exception as e:
            print(f"  ❌ Error encountered while analyzing [{sheet_name}]: {e}")
            print("🛑 Program terminates to avoid partial upload.")
            sys.exit(1)

    # 聚合总表
    if dfs_long_pool:
        df_long_all = pd.concat(dfs_long_pool, ignore_index=True)
    else:
        df_long_all = pd.DataFrame()

    if dfs_submission_pool:
        df_fact_submission_all = pd.concat(dfs_submission_pool, ignore_index=True)
    else:
        df_fact_submission_all = pd.DataFrame()

    print(f"\n📊 Aggregation Completed:")
    print(f"  -> Total records in fact_question_long: {len(df_long_all)} rows.")
    print(f"  -> Total records in fact_submission: {len(df_fact_submission_all)} rows.")

    # Step 2.3: 备用统计指标计算 (你可以根据需要决定是否推送到 Google Sheets)
    df_store_coverage = build_dim_store_coverage(df_store_master, df_fact_submission_all)
    df_country_coverage = build_country_coverage(df_store_coverage)
    df_region_coverage = build_region_coverage(df_store_coverage)

    # =============================================================================
    # 📤 3. 一键覆盖推送到 Google Sheets 云端
    # =============================================================================
    print("\n🚀 Initiating cloud sync via Google Sheets API...")
    
    # 1. 同步长表大盘
    upload_dataframe_to_google_sheet("fact_question_long", df_long_all)
    
    # 2. 同步提交统计明细
    upload_dataframe_to_google_sheet("fact_submission", df_fact_submission_all)
    
    # 3. 同步你备用的覆盖率、门店基础等分析工作表 (如果需要随时开启)
    if not df_store_coverage.empty:
        upload_dataframe_to_google_sheet("dim_store_coverage", df_store_coverage)
    if not df_country_coverage.empty:
        upload_dataframe_to_google_sheet("agg_country_coverage", df_country_coverage)
    if not df_region_coverage.empty:
        upload_dataframe_to_google_sheet("agg_region_coverage", df_region_coverage)

    print("\n==================================================")
    print("🎉 ALL SYSTEM TASKS SUCCESSFULLY EXECUTED!")
    print("==================================================")

if __name__ == "__main__":
    main()
