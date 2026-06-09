import pandas as pd
import re
import unicodedata
from datetime import datetime
from config import VERSION_MAPS, SHEET_CONFIG

def norm(s):
    """文本标准化函数：去除音标、特殊符号、多余空格并转为小写"""
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r'[^\w\s]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def transform_quickcheck_adaptive(df_raw, sheet_name):
    """
    自适应的 Wide -> Long（宽表转长表）核心清洗算法。
    自动根据 config.py 中的配置，识别 nested 结构或 simple 结构，并应用对应的分类映射。
    """
    if df_raw.empty:
        return pd.DataFrame()

    # 1. 安全补丁：清洗飞书 API 列名的前后空格
    df_raw.columns = df_raw.columns.str.strip()

    # 2. 读取该表在 config.py 中的配置，若无配置则自动激活【兜底机制】
    cfg = SHEET_CONFIG.get(sheet_name, {"structure": "simple", "category": "Quick Check"})
    struct_type = cfg["structure"]
    cat_type = cfg["category"]
    cmap = VERSION_MAPS[cat_type]

    # 3. 动态识别：找出所有符合 "\d+.\d+" 规则的问题列，以及所有的 Comment 列
    question_cols = [c for c in df_raw.columns if re.match(r'^\s*\d+\.\d+', str(c))]
    all_comment_cols = [c for c in df_raw.columns if 'comment' in str(c).lower()]

    # 4. 提取公共基础字段 (确保 Record ID 唯一，优先使用飞书自带的 id)
    # 如果原始数据里没有 Record ID，我们用行号自动生成一个
    if "Record ID" in df_raw.columns:
        id_col = "Record ID"
    elif "id" in df_raw.columns:
        id_col = "id"
    else:
        df_raw["Generated_ID"] = [f"rc_{i}" for i in range(len(df_raw))]
        id_col = "Generated_ID"

    base_cols = [id_col, "Store Name", "Submit Time"]
    if "Check Type" in df_raw.columns:
        base_cols.append("Check Type")
    elif "CheckType" in df_raw.columns:
        base_cols.append("CheckType")

    # 过滤掉原始表中不存在的列，防止报错
    base_cols = [c for c in base_cols if c in df_raw.columns]

    rows_long = []

    # 5. 核心循环：按行、按问题拆分数据
    for idx, row in df_raw.iterrows():
        rid = row[id_col]
        sname = row.get("Store Name", "")
        stime = row.get("Submit Time", "")
        
        # 兼容 Check Type 字段
        ctype = ""
        if "Check Type" in df_raw.columns: ctype = row["Check Type"]
        elif "CheckType" in df_raw.columns: ctype = row["CheckType"]

        # 处理每一道检查题
        for q_col in question_cols:
            val = row[q_col]
            if pd.isna(val) or str(val).strip() == "":
                continue

            # 提取 QuestionID (例如 "1.1")
            m = re.match(r'^\s*(\d+\.\d+)', str(q_col))
            qid_str = m.group(1) if m else "0.0"
            major_version = qid_str.split('.')[0]

            # 动态计算大类映射 (Sales, Delivery 等)
            computed_cat = cmap.get(major_version, "Unknown")

            # 动态定位对应的 Comment 列
            matched_comment_col = ""
            if struct_type == "nested":
                # Nested 结构：寻找名字里包含该问题ID的评论列，如 "1.1 comment"
                for c_col in all_comment_cols:
                    if qid_str in str(c_col):
                        matched_comment_col = c_col
                        break
            else:
                # Simple 结构：寻找干净的、没有任何数字前缀的 "Comment" 列
                for c_col in all_comment_cols:
                    if not re.search(r'\d', str(c_col)):
                        matched_comment_col = c_col
                        break

            comment_val = row[matched_comment_col] if matched_comment_col and matched_comment_col in df_raw.columns else ""

            # 完美保留你在 Colab 里设计的 Google Sheets 自动翻译公式字符串！
            # 这里的 Comment 列未来在 Google Sheets 对应的列是第 9 列 (I 列)，所以用 I
            idx_in_sheets = f"I{len(rows_long) + 2}" 
            formula_str = f'=IFERROR(GOOGLETRANSLATE({idx_in_sheets},"auto","en"),"")'

            rows_long.append({
                "RecordID": rid,
                "Store Name": sname,
                "Submit Time": stime,
                "CheckType": ctype,
                "QuestionID": float(qid_str),
                "Question": q_col,
                "Evaluation Result": val,
                "Category": computed_cat,
                "Comment": comment_val,
                "Google Translate Comment Formula": formula_str,  # 注入翻译公式
                "FormSheet": sheet_name
            })

        # 6. 处理特殊的【整体评价 / Overall Evaluation】列
        overall_cols = [c for c in df_raw.columns if 'overall evaluation' in norm(c) or 'overall score' in norm(c)]
        for o_col in overall_cols:
            o_val = row[o_col]
            if pd.isna(o_val) or str(o_val).strip() == "":
                continue

            idx_in_sheets = f"I{len(rows_long) + 2}"
            formula_str = f'=IFERROR(GOOGLETRANSLATE({idx_in_sheets},"auto","en"),"")'

            rows_long.append({
                "RecordID": rid,
                "Store Name": sname,
                "Submit Time": stime,
                "CheckType": ctype,
                "QuestionID": 0.0,
                "Question": o_col,
                "Evaluation Result": o_val,
                "Category": "Overall",
                "Comment": "",
                "Google Translate Comment Formula": formula_str,
                "FormSheet": sheet_name
            })

    return pd.DataFrame(rows_long)


def build_fact_submission(df_raw, sheet_name):
    """构建事实提交明细表 (fact_submission)"""
    if df_raw.empty:
        return pd.DataFrame()

    df_raw.columns = df_raw.columns.str.strip()

    # 标准化列名映射，防止大小写错乱
    cols = {
        "id": "Record ID" if "Record ID" in df_raw.columns else ("id" if "id" in df_raw.columns else None),
        "store": "Store Name" if "Store Name" in df_raw.columns else None,
        "time": "Submit Time" if "Submit Time" in df_raw.columns else None,
        "pass": "Pass Count" if "Pass Count" in df_raw.columns else ("Passed Count" if "Passed Count" in df_raw.columns else None),
        "fail": "Fail Count" if "Fail Count" in df_raw.columns else ("Failed Count" if "Failed Count" in df_raw.columns else None),
        "na": "N/A Count" if "N/A Count" in df_raw.columns else ("NA Count" if "NA Count" in df_raw.columns else None),
        "score": "Score" if "Score" in df_raw.columns else None
    }

    # 动态构建输出行
    rows = []
    for idx, r in df_raw.iterrows():
        rid = r[cols["id"]] if cols["id"] else f"fs_{idx}"
        
        # 解析通过数、拒绝数、总数、得分
        p_val = pd.to_numeric(r[cols["pass"]], errors='coerce') if cols["pass"] else 0
        f_val = pd.to_numeric(r[cols["fail"]], errors='coerce') if cols["fail"] else 0
        n_val = pd.to_numeric(r[cols["na"]], errors='coerce') if cols["na"] else 0
        s_val = pd.to_numeric(r[cols["score"]], errors='coerce') if cols["score"] else 0

        p_val = 0 if pd.isna(p_val) else int(p_val)
        f_val = 0 if pd.isna(f_val) else int(f_val)
        n_val = 0 if pd.isna(n_val) else int(n_val)
        s_val = 0.0 if pd.isna(s_val) else float(s_val)

        total = p_val + f_val + n_val
        rate = (p_val / (p_val + f_val)) if (p_val + f_val) > 0 else 0.0

        rows.append({
            "RecordID": rid,
            "Store Name": r[cols["store"]] if cols["store"] else "",
            "Submit Time": r[cols["time"]] if cols["time"] else "",
            "Pass Count": p_val,
            "Fail Count": f_val,
            "N/A Count": n_val,
            "Total Checked": total,
            "Pass Rate": rate,
            "Score": s_val,
            "FormSheet": sheet_name
        })

    return pd.DataFrame(rows)


def build_dim_store_coverage(df_store_master, df_fact_submission_all):
    """结合门店明细主表和大盘提交记录，构建覆盖率维度表 (dim_store_coverage)"""
    if df_store_master.empty:
        return pd.DataFrame()

    df_store_master.columns = df_store_master.columns.str.strip()
    
    # 提取有提交过记录的唯一门店集合
    submitted_stores = set()
    if not df_fact_submission_all.empty and "Store Name" in df_fact_submission_all.columns:
        submitted_stores = set(df_fact_submission_all["Store Name"].dropna().unique())

    df_store_master["Is Covered"] = df_store_master["Store Name"].apply(
        lambda x: "Covered" if x in submitted_stores else "Not Covered"
    )
    return df_store_master

# =============================================================================
# 📈 聚合计算函数 (供主程序最后统一调用生成看板大盘指标)
# =============================================================================
def build_country_coverage(df_store_coverage):
    """计算国家维度的覆盖率统计"""
    if df_store_coverage.empty: return pd.DataFrame()
    
    total = df_store_coverage.groupby("Country").size().rename("Total Stores")
    covered = df_store_coverage[df_store_coverage["Is Covered"] == "Covered"].groupby("Country").size().rename("Covered Stores")
    
    df_res = pd.concat([total, covered], axis=1).fillna(0).astype(int).reset_index()
    df_res["Coverage Rate"] = df_res["Covered Stores"] / df_res["Total Stores"]
    return df_res

def build_region_coverage(df_store_coverage):
    """计算大区维度的覆盖率统计"""
    if df_store_coverage.empty: return pd.DataFrame()
    
    total = df_store_coverage.groupby("Region").size().rename("Total Stores")
    covered = df_store_coverage[df_store_coverage["Is Covered"] == "Covered"].groupby("Region").size().rename("Covered Stores")
    
    df_res = pd.concat([total, covered], axis=1).fillna(0).astype(int).reset_index()
    df_res["Coverage Rate"] = df_res["Covered Stores"] / df_res["Total Stores"]
    return df_res
