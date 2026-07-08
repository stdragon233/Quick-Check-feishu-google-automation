# import pandas as pd
# import re
# import unicodedata
# import numpy as np

# def norm(s):
#     """文本标准化函数"""
#     if pd.isna(s):
#         return ""
#     s = str(s).lower().strip()
#     s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
#     s = re.sub(r'[^a-z0-9]', '', s)
#     return s

# def clean_feishu_cell_value(val):
#     """
#     万能解包净化器（时间戳智能适配防御版）：
#     将飞书各种单选/多选/关联/人员列表/Numpy数组，完美净化为纯文本字符串。
#     💡 新增：若发现是飞书13位毫秒级时间戳，自动智能化转换为标准时间字符串。
#     """
#     if val is None:
#         return ""
        
#     # 如果是 NumPy 数组，先转为普通列表
#     if isinstance(val, np.ndarray):
#         val = val.tolist()

#     # 处理 Pandas 的 Series 或其他多元素集合
#     if hasattr(val, '__len__') and not isinstance(val, (str, dict)):
#         if len(val) == 0:
#             return ""
#         first_item = val[0]
#         if isinstance(first_item, dict) and "name" in first_item:
#             return str(first_item["name"]).strip()
#         if isinstance(first_item, dict) and "text" in first_item:
#             return str(first_item["text"]).strip()
#         val = first_item
        
#     # 如果是单个人员/文本字典结构
#     if isinstance(val, dict):
#         if "name" in val: val = val["name"]
#         elif "text" in val: val = val["text"]
        
#     # 普通标量判断空值
#     try:
#         if pd.isna(val):
#             return ""
#     except:
#         pass
        
#     val_str = str(val).strip()
    
#     # ⏱️ 智能拦截雷达：如果是一串13位的纯数字（飞书典型的毫秒级时间戳，如 1751444817634）
#     if val_str.isdigit() and len(val_str) == 13:
#         try:
#             dt_obj = pd.to_datetime(int(val_str), unit='ms')
#             # 自动调整到当地时区（飞书API默认返回UTC，如果需要东八区北京时间，可在这里 + pd.Timedelta(hours=8)）
#             # 考虑到你面向国际化表，保持原有自然时间转换。如果之前有8小时时差，可在这里调整
#             dt_obj = dt_obj + pd.Timedelta(hours=8) # 飞书API时间通常是UTC，这里转为北京时间/或你需要的本地时间
#             return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
#         except:
#             pass

#     return val_str


# def transform_quickcheck_adaptive(df_raw, sheet_name=None, structure_version="simple", category_version="Quick Check", maps_config=None):
#     """
#     Unified transformation for XPENG Store Checks.
#     【更新版】：
#     1. 时间格式变更为 YYYY/MM/DD HH:mm:ss
#     2. 空值在最终 rows 字典中直接保留为原生的 None，不强转字符串。
#     """
#     if df_raw.empty:
#         return pd.DataFrame()

#     # 安全补丁：清洗列名前后空格
#     df_raw.columns = df_raw.columns.str.strip()

#     def get_qid_tuple(orig_qid):
#         if not orig_qid or orig_qid == "0.0" or orig_qid == "0_0":
#             return (0, 0, 0)
#         try:
#             cleaned_qid = str(orig_qid).replace('_', '.')
#             return tuple(int(p) for p in cleaned_qid.split('.'))
#         except:
#             return (999, 999, 999)

#     if maps_config is None:
#         maps_config = {
#             "Quick Check": {"1": "Sales", "2": "Delivery", "3": "Aftersales", "4": "Marketing&UserOperation", "5": "StaffDemeanor"}
#         }

#     qtype_map = maps_config.get(category_version, maps_config.get("Quick Check"))

#     all_q_cols = [c for c in df_raw.columns if re.match(r'^\s*\d+\.\d+', str(c))]
#     all_comment_cols = [c for c in df_raw.columns if 'comment' in str(c).lower()]
#     overall_eval_cols = [c for c in df_raw.columns if "overall evaluation" in str(c).lower()]
#     overall_eval_col = overall_eval_cols[0] if overall_eval_cols else None

#     comment_norm = {c: norm(c) for c in all_comment_cols}
#     rows = []

#     id_col = None
#     for candidate in ['Record Number', 'Record ID', 'id']:
#         if candidate in df_raw.columns:
#             id_col = candidate
#             break
    
#     for idx, row in df_raw.iterrows():
#         result_id = clean_feishu_cell_value(row[id_col]) if id_col else f"rc_{idx}"
        
#         submission_time = row.get('Submission Time') if 'Submission Time' in row else row.get('SubmissionTime')
#         if submission_time is None:
#             submission_time = row.get('Submit Time', None)
        
#         submission_time = clean_feishu_cell_value(submission_time)
        
#         # ⏱️ 核心修改：将获取的时间连带格式转化为 2026/02/10 14:08:14 形式
#         if submission_time:
#             try:
#                 submission_time = pd.to_datetime(submission_time).strftime('%Y/%m/%d %H:%M:%S')
#             except:
#                 pass
#         else:
#             submission_time = None  # 找不到时间则保持 None

#         check_type = str(clean_feishu_cell_value(row.get("Check Type", "Self-check"))).strip()
#         if check_type in ["", "nan", "None"]:
#             check_type = "Self-check"

#         region = clean_feishu_cell_value(row.get("Region", None)) or None
#         country_region = clean_feishu_cell_value(row.get("Country/region", row.get("Country/Region", None))) or None
#         city = clean_feishu_cell_value(row.get("City", None)) or None
#         store_name = clean_feishu_cell_value(row.get("Store Name", None)) or None
#         creator = clean_feishu_cell_value(row.get("Creator", None)) or None

#         current_parent_text = ""

#         for qcol in all_q_cols:
#             m = re.match(r'^\s*(\d+\.\d+(?:\.\d+)?)\s*(.*)$', str(qcol))
#             if not m: continue

#             qid = m.group(1)
#             col_text = m.group(2).strip()
#             is_sub_question = len(qid.split('.')) == 3

#             if structure_version == "nested":
#                 if not is_sub_question:
#                     current_parent_text = col_text if col_text else qcol
#                     next_col_idx = all_q_cols.index(qcol) + 1
#                     has_children = False
#                     if next_col_idx < len(all_q_cols):
#                         next_m = re.match(r'^\s*(\d+\.\d+(?:\.\d+)?)', str(all_q_cols[next_col_idx]))
#                         if next_m and next_m.group(1).startswith(qid + "."):
#                             has_children = True

#                     if has_children: continue
#                     target_qid, target_qtext = qid, current_parent_text
#                     match_key = norm(current_parent_text)
#                 else:
#                     target_qid, target_qtext = qid, col_text if col_text else qcol
#                     match_key = norm(current_parent_text)
#             else:
#                 target_qid, target_qtext = qid, col_text if col_text else qcol
#                 match_key = norm(target_qtext)

#             comment_val = None
#             if match_key:
#                 matched_comments = [c for c, cn in comment_norm.items() if match_key in cn]
#                 for c in matched_comments:
#                     v = row.get(c)
#                     cleaned_v = clean_feishu_cell_value(v)
#                     if cleaned_v != "":
#                         comment_val = cleaned_v
#                         break

#             result_val = row.get(qcol, None)
#             result_val = clean_feishu_cell_value(result_val)
#             if result_val == "": continue

#             qtype_digit = qid.split('.')[0]
#             qtype = qtype_map.get(qtype_digit, "Other")

#             clean_underline_qid = str(target_qid).replace('.', '_')
#             sort_tuple = get_qid_tuple(target_qid)

#             # 保持基础结构，如果某个单元格内容彻底为空，允许推入 None 供飞书和 Google 置空
#             rows.append({
#                 "RecordID": f"{result_id}_{clean_underline_qid}",
#                 "ResultID": result_id,
#                 "SubmissionTime": submission_time,
#                 "CheckType": check_type, 
#                 "Region": region, 
#                 "Country/Region": country_region,
#                 "City": city, 
#                 "StoreName": store_name, 
#                 "Creator": creator,
#                 "QuestionID": clean_underline_qid,
#                 "QuestionSortKey": sort_tuple,
#                 "QuestionType": qtype, 
#                 "QuestionText": target_qtext,
#                 "Result": result_val if result_val != "" else None, 
#                 "Comment": comment_val,
#                 "FormSheet": sheet_name
#             })

#         if overall_eval_col:
#             overall_comment = clean_feishu_cell_value(row.get(overall_eval_col))
#             if overall_comment != "":
#                 rows.append({
#                     "RecordID": f"{result_id}_0_0",
#                     "ResultID": result_id, 
#                     "SubmissionTime": submission_time,
#                     "CheckType": check_type, 
#                     "Region": region, 
#                     "Country/Region": country_region,
#                     "City": city, 
#                     "StoreName": store_name, 
#                     "Creator": creator,
#                     "QuestionID": "0_0",
#                     "QuestionSortKey": (0, 0, 0),
#                     "QuestionType": "Overall Evaluation", 
#                     "QuestionText": "Overall Evaluation",
#                     "Result": None, 
#                     "Comment": overall_comment,
#                     "FormSheet": sheet_name
#                 })

#     df_long = pd.DataFrame(rows)

#     if not df_long.empty:
#         df_long = df_long.sort_values(by=['ResultID', 'QuestionSortKey']).reset_index(drop=True)
#         df_long = df_long.drop(columns=["QuestionSortKey"])

#         if "Comment" in df_long.columns:
#             orig_comm_idx = df_long.columns.get_loc("Comment")
#             n = orig_comm_idx
#             col_letter = ""
#             while n >= 0:
#                 col_letter = chr(n % 26 + 65) + col_letter
#                 n = n // 26 - 1
#             df_long.insert(orig_comm_idx + 1, "Comment_AutoTranslate", "")
#             df_long["Comment_AutoTranslate"] = [f'=IFERROR(GOOGLETRANSLATE(INDIRECT("{col_letter}"&ROW()),"auto","en"),"")' for _ in range(len(df_long))]

#     return df_long


# def build_fact_submission(df_raw, form_sheet_name=None):
#     """
#     Build 1-row-per-submission fact table using form-calculated metrics.
#     【更新版】：
#     1. 日期格式输出完全对齐：YYYY/MM/DD HH:mm:ss
#     2. 所有的 PassRate（通过率）均保留为原生纯浮点数（如 0.855），去除末尾的 "%"
#     3. 所有的 Count（计数）均保留为纯整数，若为空则返回 None，绝不强制填 0。
#     """
#     if df_raw.empty:
#         return pd.DataFrame()

#     df_raw.columns = df_raw.columns.str.strip()

#     id_col = None
#     for candidate in ['Record Number', 'Record ID', 'id']:
#         if candidate in df_raw.columns:
#             id_col = candidate
#             break

#     orig_time_col = "Submission Time" if "Submission Time" in df_raw.columns else ("SubmissionTime" if "SubmissionTime" in df_raw.columns else "Submit Time")
    
#     # 统一过一遍安全清洗器并转换为标准的 Datetime 对象
#     if orig_time_col in df_raw.columns:
#         df_raw[orig_time_col] = df_raw[orig_time_col].apply(clean_feishu_cell_value)
#         df_raw[orig_time_col] = pd.to_datetime(df_raw[orig_time_col], errors='coerce')
#     else:
#         df_raw[orig_time_col] = pd.NaT

#     has_check_type_col = "Check Type" in df_raw.columns or "CheckType" in df_raw.columns
#     check_type_field = "Check Type" if "Check Type" in df_raw.columns else "CheckType"

#     section_map = {
#         "Sales": {
#             "rate": "Sales Pass Rate",
#             "pass": "Sales Pass Count",
#             "nopass": "Sales NoPass Count",
#             "na": "Sales NA Count"
#         },
#         "Delivery": {
#             "rate": "Delivery Pass Rate",
#             "pass": "Delivery Pass Count",
#             "nopass": "Delivery NoPass Count",
#             "na": "Delivery NA Count"
#         },
#         "Aftersales": {
#             "rate": "Aftersales Pass Rate",
#             "pass": "Aftersales Pass Count",
#             "nopass": "Aftersales NoPass Count",
#             "na": "Aftersales NA Count"
#         },
#         "Marketing": {
#             "rate": "Marketing Pass Rate",
#             "pass": "Marketing Pass Count",
#             "nopass": "Marketing NoPass Count",
#             "na": "Marketing NA Count"
#         }
#     }

#     rows = []

#     for _, r in df_raw.iterrows():
#         submission_time = r.get(orig_time_col)
#         has_time = pd.notna(submission_time)

#         if has_check_type_col:
#             check_type = clean_feishu_cell_value(r.get(check_type_field))
#             if check_type in ["", "nan", "None"]:
#                 check_type = "Self-check"
#             else:
#                 check_type = str(check_type).strip()
#         else:
#             check_type = "Self-check"

#         sub_id = clean_feishu_cell_value(r.get(id_col)) if id_col else f"fs_{_}"
#         creator = clean_feishu_cell_value(r.get("Creator", None)) or None
#         region = clean_feishu_cell_value(r.get("Region", None)) or None
#         country_region = clean_feishu_cell_value(r.get("Country/region", r.get("Country/Region", None))) or None
#         city = clean_feishu_cell_value(r.get("City", None)) or None
#         store_name = clean_feishu_cell_value(r.get("Store Name", None)) or None

#         row = {
#             "SubmissionID": sub_id,
#             # ⏱️ 修改点：在此处先将 submission_time 转换为标准格式文本存入字典，防止后面被 pandas 默认格式覆盖
#             "SubmissionTime": submission_time.strftime("%Y/%m/%d %H:%M:%S") if has_time else None,
#             "CheckType": check_type,
#             "Creator": creator,
#             "Region": region,
#             "Country/Region": country_region,
#             "City": city,
#             "StoreName": store_name,
#             "FormSheet": form_sheet_name,

#             "YearMonth": submission_time.strftime("%Y%m") if has_time else None,
#             "Quarter": f"{submission_time.year} Q{submission_time.quarter}" if has_time else None,
#             "Submission Count": 1
#         }

#         total_pass = 0
#         total_nopass = 0
#         total_na = 0
#         has_any_count = False # 用来标记这一行是否有有效计数数据

#         for section, cols in section_map.items():
#             prefix = section.replace(" ", "")

#             pass_raw = clean_feishu_cell_value(r.get(cols["pass"]))
#             nopass_raw = clean_feishu_cell_value(r.get(cols["nopass"]))
#             na_raw = clean_feishu_cell_value(r.get(cols["na"]))
#             rate_raw = clean_feishu_cell_value(r.get(cols["rate"]))

#             pass_cnt = pd.to_numeric(pass_raw, errors='coerce')
#             nopass_cnt = pd.to_numeric(nopass_raw, errors='coerce')
#             na_cnt = pd.to_numeric(na_raw, errors='coerce')

#             # 🛑 核心逻辑：只有当这三个计数不全为 NaN 时，才代表这一板块有数据
#             if pd.notna(pass_cnt) or pd.notna(nopass_cnt) or pd.notna(na_cnt):
#                 has_any_count = True
#                 p_val = 0.0 if pd.isna(pass_cnt) else float(pass_cnt)
#                 np_val = 0.0 if pd.isna(nopass_cnt) else float(nopass_cnt)
#                 n_val = 0.0 if pd.isna(na_cnt) else float(na_cnt)

#                 total_pass += p_val
#                 total_nopass += np_val
#                 total_na += n_val

#                 # 📊 处理四大板块的通过率：如果是文本 "85.5%" 转换为纯小数 0.855
#                 parsed_rate = None
#                 if rate_raw and "%" in str(rate_raw):
#                     try:
#                         parsed_rate = float(str(rate_raw).replace('%', '').strip()) / 100.0
#                     except:
#                         pass
#                 elif rate_raw != "":
#                     try:
#                         parsed_rate = float(rate_raw)
#                         if parsed_rate > 1.0: # 防御：如果是 85.5 这种没带%但按百算的
#                             parsed_rate = parsed_rate / 100.0
#                     except:
#                         pass

#                 row[f"{prefix}_PassRate"] = parsed_rate
#                 row[f"{prefix}_PassCount"] = int(p_val)
#                 row[f"{prefix}_NoPassCount"] = int(np_val)
#                 row[f"{prefix}_NACount"] = int(n_val)
#                 row[f"{prefix}_SubmissionCount"] = 1 if (p_val + np_val + n_val) > 0 else 0
#             else:
#                 # 🪐 如果该板块彻底没数据，全部填 None
#                 row[f"{prefix}_PassRate"] = None
#                 row[f"{prefix}_PassCount"] = None
#                 row[f"{prefix}_NoPassCount"] = None
#                 row[f"{prefix}_NACount"] = None
#                 row[f"{prefix}_SubmissionCount"] = None

#         total_count = total_pass + total_nopass + total_na

#         # 📊 计算 Overall 通过率，不再转成带 % 的文本，而是保留原生浮点数
#         if has_any_count and total_count > 0:
#             rate_val = (total_pass + total_na) / total_count
#             row["Overall_PassRate"] = float(round(rate_val, 4)) # 保留纯小数
#             row["Overall_Performance"] = f"({int(total_pass + total_na)} / {int(total_count)})"
#         else:
#             row["Overall_PassRate"] = None # 没数据就保持 None，不写 0.00%
#             row["Overall_Performance"] = None

#         rows.append(row)

#     df = pd.DataFrame(rows)
#     if df.empty:
#         return df

#     # ⏱️ 移除原有的 pd.to_datetime 强转，因为我们在字典里就已经把时间定义为了完美的"2026/02/10 14:08:14"格式文本
#     # 如果用 pandas 默认转，它在 to_csv 或推给 google 时又会自作聪明变回减号 "-" 格式。

#     extra_cols = [
#         "YearMonth", "Quarter", "Submission Count",
#         "Sales_SubmissionCount", "Delivery_SubmissionCount",
#         "Aftersales_SubmissionCount", "Marketing_SubmissionCount"
#     ]
#     base_cols = [
#         "SubmissionID", "SubmissionTime", "CheckType", "Creator",
#         "Region", "Country/Region", "City", "StoreName", "FormSheet"
#     ]
#     overall_cols = ["Overall_PassRate", "Overall_Performance"]

#     section_cols = []
#     for section in section_map.keys():
#         prefix = section.replace(" ", "")
#         section_cols.extend([f"{prefix}_PassRate", f"{prefix}_PassCount", f"{prefix}_NoPassCount", f"{prefix}_NACount"])

#     final_order = base_cols + overall_cols + section_cols + extra_cols
#     final_order = [c for c in final_order if c in df.columns]
#     df = df[final_order]

#     return df


# def enrich_store_info(df_long, df_submission, store_master_df):
#     """
#     使用门店明细表，修正 long 表和 submission 表中的门店地理位置信息
#     根据 StoreName 匹配门店明细表中的 Region、Country、City
#     匹配失败则设为 None（留空）
    
#     Args:
#         df_long: fact_question_long DataFrame
#         df_submission: fact_submission DataFrame
#         store_master_df: 门店明细表原始数据（需包含 Store Name, Region, Country, City 列）
    
#     Returns:
#         (df_long_enriched, df_submission_enriched)
#     """
#     if store_master_df.empty:
#         print("⚠️ 门店明细表为空，跳过地理位置信息修正")
#         return df_long, df_submission
    
#     # 检查门店明细表必要的列
#     required_cols = ['Store Name', 'Region', 'Country', 'City']
#     missing_cols = [col for col in required_cols if col not in store_master_df.columns]
#     if missing_cols:
#         print(f"❌ 门店明细表缺少必要列: {missing_cols}，跳过修正")
#         return df_long, df_submission
    
#     # 构建映射字典: Store Name -> {Region, Country, City}
#     print("📋 正在构建门店信息映射字典...")
#     store_mapping = {}
#     for _, row in store_master_df.iterrows():
#         store_name = row.get('Store Name')
#         if pd.isna(store_name) or str(store_name).strip() == '':
#             continue
        
#         store_key = str(store_name).strip()
#         store_mapping[store_key] = {
#             'Region': row.get('Region') if not pd.isna(row.get('Region')) else None,
#             'Country': row.get('Country') if not pd.isna(row.get('Country')) else None,
#             'City': row.get('City') if not pd.isna(row.get('City')) else None
#         }
    
#     print(f"✅ 门店映射字典构建完成，共 {len(store_mapping)} 个门店")
    
#     # 辅助函数：根据门店名称获取字段值
#     def get_store_field(store_name, field_name):
#         if pd.isna(store_name) or str(store_name).strip() == '':
#             return None
#         store_key = str(store_name).strip()
#         if store_key in store_mapping:
#             return store_mapping[store_key].get(field_name)
#         return None
    
#     # 修正 submission 表
#     if not df_submission.empty and 'StoreName' in df_submission.columns:
#         print("🔧 正在修正 Submission Facts 表的地理位置信息...")
        
#         original_count = len(df_submission)
#         # 记录修正前的非空数量
#         region_before = df_submission['Region'].notna().sum()
#         country_before = df_submission['Country/Region'].notna().sum()
#         city_before = df_submission['City'].notna().sum()
        
#         # 应用映射
#         df_submission['Region'] = df_submission['StoreName'].apply(
#             lambda x: get_store_field(x, 'Region')
#         )
#         df_submission['Country/Region'] = df_submission['StoreName'].apply(
#             lambda x: get_store_field(x, 'Country')
#         )
#         df_submission['City'] = df_submission['StoreName'].apply(
#             lambda x: get_store_field(x, 'City')
#         )
        
#         # 统计匹配情况
#         matched_count = df_submission['StoreName'].apply(
#             lambda x: str(x).strip() if pd.notna(x) else None
#         ).isin(store_mapping.keys()).sum()
        
#         print(f"   ✅ Submission Facts: {original_count} 行")
#         print(f"      - 匹配成功: {matched_count} 行 ({matched_count/original_count*100:.1f}%)")
#         print(f"      - Region: {region_before} → {df_submission['Region'].notna().sum()} 个非空值")
#         print(f"      - Country/Region: {country_before} → {df_submission['Country/Region'].notna().sum()} 个非空值")
#         print(f"      - City: {city_before} → {df_submission['City'].notna().sum()} 个非空值")
        
#         # 列出匹配失败的门店（去重）
#         unmatched_stores = set()
#         for store_name in df_submission['StoreName']:
#             if pd.notna(store_name):
#                 store_key = str(store_name).strip()
#                 if store_key not in store_mapping:
#                     unmatched_stores.add(store_key)
        
#         if unmatched_stores:
#             print(f"      ⚠️ 未匹配到的门店 ({len(unmatched_stores)} 个): {list(unmatched_stores)[:10]}")
#             if len(unmatched_stores) > 10:
#                 print(f"         ... 等共 {len(unmatched_stores)} 个门店")
#     else:
#         print("⚠️ Submission Facts 表为空或缺少 StoreName 列，跳过修正")
    
#     # 修正 long 表
#     if not df_long.empty and 'StoreName' in df_long.columns:
#         print("🔧 正在修正 fact_question_long 表的地理位置信息...")
        
#         original_count = len(df_long)
#         region_before = df_long['Region'].notna().sum()
#         country_before = df_long['Country/Region'].notna().sum()
#         city_before = df_long['City'].notna().sum()
        
#         # 应用映射
#         df_long['Region'] = df_long['StoreName'].apply(
#             lambda x: get_store_field(x, 'Region')
#         )
#         df_long['Country/Region'] = df_long['StoreName'].apply(
#             lambda x: get_store_field(x, 'Country')
#         )
#         df_long['City'] = df_long['StoreName'].apply(
#             lambda x: get_store_field(x, 'City')
#         )
        
#         matched_count = df_long['StoreName'].apply(
#             lambda x: str(x).strip() if pd.notna(x) else None
#         ).isin(store_mapping.keys()).sum()
        
#         print(f"   ✅ fact_question_long: {original_count} 行")
#         print(f"      - 匹配成功: {matched_count} 行 ({matched_count/original_count*100:.1f}%)")
#         print(f"      - Region: {region_before} → {df_long['Region'].notna().sum()} 个非空值")
#         print(f"      - Country/Region: {country_before} → {df_long['Country/Region'].notna().sum()} 个非空值")
#         print(f"      - City: {city_before} → {df_long['City'].notna().sum()} 个非空值")
#     else:
#         print("⚠️ fact_question_long 表为空或缺少 StoreName 列，跳过修正")
    
#     print("✅ 门店地理位置信息修正完成")
#     return df_long, df_submission

import pandas as pd
import re
import unicodedata
import numpy as np

def norm(s):
    """文本标准化函数"""
    if pd.isna(s):
        return ""
    s = str(s).lower().strip()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

def clean_feishu_cell_value(val):
    """
    万能解包净化器（时间戳智能适配防御版）：
    将飞书各种单选/多选/关联/人员列表/Numpy数组，完美净化为纯文本字符串。
    💡 新增：若发现是飞书13位毫秒级时间戳，自动智能化转换为标准时间字符串。
    """
    if val is None:
        return ""
        
    # 如果是 NumPy 数组，先转为普通列表
    if isinstance(val, np.ndarray):
        val = val.tolist()

    # 处理 Pandas 的 Series 或其他多元素集合
    if hasattr(val, '__len__') and not isinstance(val, (str, dict)):
        if len(val) == 0:
            return ""
        first_item = val[0]
        if isinstance(first_item, dict) and "name" in first_item:
            return str(first_item["name"]).strip()
        if isinstance(first_item, dict) and "text" in first_item:
            return str(first_item["text"]).strip()
        val = first_item
        
    # 如果是单个人员/文本字典结构
    if isinstance(val, dict):
        if "name" in val: val = val["name"]
        elif "text" in val: val = val["text"]
        
    # 普通标量判断空值
    try:
        if pd.isna(val):
            return ""
    except:
        pass
        
    val_str = str(val).strip()
    
    # ⏱️ 智能拦截雷达：如果是一串13位的纯数字（飞书典型的毫秒级时间戳，如 1751444817634）
    if val_str.isdigit() and len(val_str) == 13:
        try:
            dt_obj = pd.to_datetime(int(val_str), unit='ms')
            dt_obj = dt_obj + pd.Timedelta(hours=8) # 飞书API时间通常是UTC，这里转为北京时间
            return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass

    return val_str


def transform_quickcheck_adaptive(df_raw, sheet_name=None, structure_version="simple", category_version="Quick Check", maps_config=None):
    """
    Unified transformation for XPENG Store Checks.
    """
    if df_raw.empty:
        return pd.DataFrame()

    df_raw.columns = df_raw.columns.str.strip()

    def get_qid_tuple(orig_qid):
        if not orig_qid or orig_qid == "0.0" or orig_qid == "0_0":
            return (0, 0, 0)
        try:
            cleaned_qid = str(orig_qid).replace('_', '.')
            return tuple(int(p) for p in cleaned_qid.split('.'))
        except:
            return (999, 999, 999)

    if maps_config is None:
        maps_config = {
            "Quick Check": {"1": "Sales", "2": "Delivery", "3": "Aftersales", "4": "Marketing&UserOperation", "5": "StaffDemeanor"}
        }

    qtype_map = maps_config.get(category_version, maps_config.get("Quick Check"))

    all_q_cols = [c for c in df_raw.columns if re.match(r'^\s*\d+\.\d+', str(c))]
    all_comment_cols = [c for c in df_raw.columns if 'comment' in str(c).lower()]
    overall_eval_cols = [c for c in df_raw.columns if "overall evaluation" in str(c).lower()]
    overall_eval_col = overall_eval_cols[0] if overall_eval_cols else None

    comment_norm = {c: norm(c) for c in all_comment_cols}
    rows = []

    id_col = None
    for candidate in ['Record Number', 'Record ID', 'id']:
        if candidate in df_raw.columns:
            id_col = candidate
            break
    
    for idx, row in df_raw.iterrows():
        result_id = clean_feishu_cell_value(row[id_col]) if id_col else f"rc_{idx}"
        
        submission_time = row.get('Submission Time') if 'Submission Time' in row else row.get('SubmissionTime')
        if submission_time is None:
            submission_time = row.get('Submit Time', None)
        
        submission_time = clean_feishu_cell_value(submission_time)
        
        if submission_time:
            try:
                submission_time = pd.to_datetime(submission_time).strftime('%Y/%m/%d %H:%M:%S')
            except:
                pass
        else:
            submission_time = None

        check_type = str(clean_feishu_cell_value(row.get("Check Type", "Self-check"))).strip()
        if check_type in ["", "nan", "None"]:
            check_type = "Self-check"

        region = clean_feishu_cell_value(row.get("Region", None)) or None
        country_region = clean_feishu_cell_value(row.get("Country/region", row.get("Country/Region", None))) or None
        city = clean_feishu_cell_value(row.get("City", None)) or None
        store_name = clean_feishu_cell_value(row.get("Store Name", None)) or None
        creator = clean_feishu_cell_value(row.get("Creator", None)) or None

        current_parent_text = ""

        for qcol in all_q_cols:
            m = re.match(r'^\s*(\d+\.\d+(?:\.\d+)?)\s*(.*)$', str(qcol))
            if not m: continue

            qid = m.group(1)
            col_text = m.group(2).strip()
            is_sub_question = len(qid.split('.')) == 3

            if structure_version == "nested":
                if not is_sub_question:
                    current_parent_text = col_text if col_text else qcol
                    next_col_idx = all_q_cols.index(qcol) + 1
                    has_children = False
                    if next_col_idx < len(all_q_cols):
                        next_m = re.match(r'^\s*(\d+\.\d+(?:\.\d+)?)', str(all_q_cols[next_col_idx]))
                        if next_m and next_m.group(1).startswith(qid + "."):
                            has_children = True

                    if has_children: continue
                    target_qid, target_qtext = qid, current_parent_text
                    match_key = norm(current_parent_text)
                else:
                    target_qid, target_qtext = qid, col_text if col_text else qcol
                    match_key = norm(current_parent_text)
            else:
                target_qid, target_qtext = qid, col_text if col_text else qcol
                match_key = norm(target_qtext)

            comment_val = None
            if match_key:
                matched_comments = [c for c, cn in comment_norm.items() if match_key in cn]
                for c in matched_comments:
                    v = row.get(c)
                    cleaned_v = clean_feishu_cell_value(v)
                    if cleaned_v != "":
                        comment_val = cleaned_v
                        break

            result_val = row.get(qcol, None)
            result_val = clean_feishu_cell_value(result_val)
            if result_val == "": continue

            qtype_digit = qid.split('.')[0]
            qtype = qtype_map.get(qtype_digit, "Other")

            clean_underline_qid = str(target_qid).replace('.', '_')
            sort_tuple = get_qid_tuple(target_qid)

            rows.append({
                "RecordID": f"{result_id}_{clean_underline_qid}",
                "ResultID": result_id,
                "SubmissionTime": submission_time,
                "CheckType": check_type, 
                "Region": region, 
                "Country/Region": country_region,
                "City": city, 
                "StoreName": store_name, 
                "Creator": creator,
                "QuestionID": clean_underline_qid,
                "QuestionSortKey": sort_tuple,
                "QuestionType": qtype, 
                "QuestionText": target_qtext,
                "Result": result_val if result_val != "" else None, 
                "Comment": comment_val,
                "FormSheet": sheet_name
            })

        if overall_eval_col:
            overall_comment = clean_feishu_cell_value(row.get(overall_eval_col))
            if overall_comment != "":
                rows.append({
                    "RecordID": f"{result_id}_0_0",
                    "ResultID": result_id, 
                    "SubmissionTime": submission_time,
                    "CheckType": check_type, 
                    "Region": region, 
                    "Country/Region": country_region,
                    "City": city, 
                    "StoreName": store_name, 
                    "Creator": creator,
                    "QuestionID": "0_0",
                    "QuestionSortKey": (0, 0, 0),
                    "QuestionType": "Overall Evaluation", 
                    "QuestionText": "Overall Evaluation",
                    "Result": None, 
                    "Comment": overall_comment,
                    "FormSheet": sheet_name
                })

    df_long = pd.DataFrame(rows)

    if not df_long.empty:
        df_long = df_long.sort_values(by=['ResultID', 'QuestionSortKey']).reset_index(drop=True)
        df_long = df_long.drop(columns=["QuestionSortKey"])

        if "Comment" in df_long.columns:
            orig_comm_idx = df_long.columns.get_loc("Comment")
            n = orig_comm_idx
            col_letter = ""
            while n >= 0:
                col_letter = chr(n % 26 + 65) + col_letter
                n = n // 26 - 1
            df_long.insert(orig_comm_idx + 1, "Comment_AutoTranslate", "")
            df_long["Comment_AutoTranslate"] = [f'=IFERROR(GOOGLETRANSLATE(INDIRECT("{col_letter}"&ROW()),"auto","en"),"")' for _ in range(len(df_long))]

    return df_long


def build_fact_submission(df_raw, form_sheet_name=None):
    """
    Build 1-row-per-submission fact table using form-calculated metrics.
    【更新版】：
    1. 日期格式输出完全对齐：YYYY/MM/DD HH:mm:ss
    2. 所有的 PassRate（通过率）均保留为原生纯浮点数（如 0.855），去除末尾的 "%"
    3. Overall_PassRate 与 Overall_Performance 直接映射数据源中的 "Total Pass Rate" 与 "Total Overview"
    """
    if df_raw.empty:
        return pd.DataFrame()

    df_raw.columns = df_raw.columns.str.strip()

    id_col = None
    for candidate in ['Record Number', 'Record ID', 'id']:
        if candidate in df_raw.columns:
            id_col = candidate
            break

    orig_time_col = "Submission Time" if "Submission Time" in df_raw.columns else ("SubmissionTime" if "SubmissionTime" in df_raw.columns else "Submit Time")
    
    if orig_time_col in df_raw.columns:
        df_raw[orig_time_col] = df_raw[orig_time_col].apply(clean_feishu_cell_value)
        df_raw[orig_time_col] = pd.to_datetime(df_raw[orig_time_col], errors='coerce')
    else:
        df_raw[orig_time_col] = pd.NaT

    has_check_type_col = "Check Type" in df_raw.columns or "CheckType" in df_raw.columns
    check_type_field = "Check Type" if "Check Type" in df_raw.columns else "CheckType"

    section_map = {
        "Sales": {"rate": "Sales Pass Rate", "pass": "Sales Pass Count", "nopass": "Sales NoPass Count", "na": "Sales NA Count"},
        "Delivery": {"rate": "Delivery Pass Rate", "pass": "Delivery Pass Count", "nopass": "Delivery NoPass Count", "na": "Delivery NA Count"},
        "Aftersales": {"rate": "Aftersales Pass Rate", "pass": "Aftersales Pass Count", "nopass": "Aftersales NoPass Count", "na": "Aftersales NA Count"},
        "Marketing": {"rate": "Marketing Pass Rate", "pass": "Marketing Pass Count", "nopass": "Marketing NoPass Count", "na": "Marketing NA Count"}
    }

    rows = []

    for _, r in df_raw.iterrows():
        submission_time = r.get(orig_time_col)
        has_time = pd.notna(submission_time)

        if has_check_type_col:
            check_type = clean_feishu_cell_value(r.get(check_type_field))
            if check_type in ["", "nan", "None"]:
                check_type = "Self-check"
            else:
                check_type = str(check_type).strip()
        else:
            check_type = "Self-check"

        sub_id = clean_feishu_cell_value(r.get(id_col)) if id_col else f"fs_{_}"
        creator = clean_feishu_cell_value(r.get("Creator", None)) or None
        region = clean_feishu_cell_value(r.get("Region", None)) or None
        country_region = clean_feishu_cell_value(r.get("Country/region", r.get("Country/Region", None))) or None
        city = clean_feishu_cell_value(r.get("City", None)) or None
        store_name = clean_feishu_cell_value(r.get("Store Name", None)) or None

        row = {
            "SubmissionID": sub_id,
            "SubmissionTime": submission_time.strftime("%Y/%m/%d %H:%M:%S") if has_time else None,
            "CheckType": check_type,
            "Creator": creator,
            "Region": region,
            "Country/Region": country_region,
            "City": city,
            "StoreName": store_name,
            "FormSheet": form_sheet_name,
            "YearMonth": submission_time.strftime("%Y%m") if has_time else None,
            "Quarter": f"{submission_time.year} Q{submission_time.quarter}" if has_time else None,
            "Submission Count": 1
        }

        for section, cols in section_map.items():
            prefix = section.replace(" ", "")

            pass_raw = clean_feishu_cell_value(r.get(cols["pass"]))
            nopass_raw = clean_feishu_cell_value(r.get(cols["nopass"]))
            na_raw = clean_feishu_cell_value(r.get(cols["na"]))
            rate_raw = clean_feishu_cell_value(r.get(cols["rate"]))

            pass_cnt = pd.to_numeric(pass_raw, errors='coerce')
            nopass_cnt = pd.to_numeric(nopass_raw, errors='coerce')
            na_cnt = pd.to_numeric(na_raw, errors='coerce')

            if pd.notna(pass_cnt) or pd.notna(nopass_cnt) or pd.notna(na_cnt):
                p_val = 0.0 if pd.isna(pass_cnt) else float(pass_cnt)
                np_val = 0.0 if pd.isna(nopass_cnt) else float(nopass_cnt)
                n_val = 0.0 if pd.isna(na_cnt) else float(na_cnt)

                parsed_rate = None
                if rate_raw and "%" in str(rate_raw):
                    try:
                        parsed_rate = float(str(rate_raw).replace('%', '').strip()) / 100.0
                    except:
                        pass
                elif rate_raw != "":
                    try:
                        parsed_rate = float(rate_raw)
                        if parsed_rate > 1.0:
                            parsed_rate = parsed_rate / 100.0
                    except:
                        pass

                row[f"{prefix}_PassRate"] = parsed_rate
                row[f"{prefix}_PassCount"] = int(p_val)
                row[f"{prefix}_NoPassCount"] = int(np_val)
                row[f"{prefix}_NACount"] = int(n_val)
                row[f"{prefix}_SubmissionCount"] = 1 if (p_val + np_val + n_val) > 0 else 0
            else:
                row[f"{prefix}_PassRate"] = None
                row[f"{prefix}_PassCount"] = None
                row[f"{prefix}_NoPassCount"] = None
                row[f"{prefix}_NACount"] = None
                row[f"{prefix}_SubmissionCount"] = None

        # 📊 直接从原始列中清洗并提取 Overall_PassRate
        total_rate_raw = clean_feishu_cell_value(r.get("Total Pass Rate"))
        overall_pass_rate = None
        if total_rate_raw and "%" in str(total_rate_raw):
            try:
                overall_pass_rate = float(str(total_rate_raw).replace('%', '').strip()) / 100.0
            except:
                pass
        elif total_rate_raw != "":
            try:
                overall_pass_rate = float(total_rate_raw)
                if overall_pass_rate > 1.0:
                    overall_pass_rate = overall_pass_rate / 100.0
            except:
                pass

        row["Overall_PassRate"] = overall_pass_rate if overall_pass_rate is not None else None

        # 📊 直接读取原始列中的 Total Overview 为 Performance 文本
        total_overview_raw = clean_feishu_cell_value(r.get("Total Overview"))
        row["Overall_Performance"] = total_overview_raw if total_overview_raw != "" else None

        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    extra_cols = [
        "YearMonth", "Quarter", "Submission Count",
        "Sales_SubmissionCount", "Delivery_SubmissionCount",
        "Aftersales_SubmissionCount", "Marketing_SubmissionCount"
    ]
    base_cols = [
        "SubmissionID", "SubmissionTime", "CheckType", "Creator",
        "Region", "Country/Region", "City", "StoreName", "FormSheet"
    ]
    overall_cols = ["Overall_PassRate", "Overall_Performance"]

    section_cols = []
    for section in section_map.keys():
        prefix = section.replace(" ", "")
        section_cols.extend([f"{prefix}_PassRate", f"{prefix}_PassCount", f"{prefix}_NoPassCount", f"{prefix}_NACount"])

    final_order = base_cols + overall_cols + section_cols + extra_cols
    final_order = [c for c in final_order if c in df.columns]
    df = df[final_order]

    return df


def enrich_store_info(df_long, df_submission, store_master_df):
    """
    使用门店明细表，修正 long 表和 submission 表中的门店地理位置信息
    """
    if store_master_df.empty:
        print("⚠️ 门店明细表为空，跳过地理位置信息修正")
        return df_long, df_submission
    
    required_cols = ['Store Name', 'Region', 'Country', 'City']
    missing_cols = [col for col in required_cols if col not in store_master_df.columns]
    if missing_cols:
        print(f"❌ 门店明细表缺少必要列: {missing_cols}，跳过修正")
        return df_long, df_submission
    
    print("📋 正在构建门店信息映射字典...")
    store_mapping = {}
    for _, row in store_master_df.iterrows():
        store_name = row.get('Store Name')
        if pd.isna(store_name) or str(store_name).strip() == '':
            continue
        
        store_key = str(store_name).strip()
        store_mapping[store_key] = {
            'Region': row.get('Region') if not pd.isna(row.get('Region')) else None,
            'Country': row.get('Country') if not pd.isna(row.get('Country')) else None,
            'City': row.get('City') if not pd.isna(row.get('City')) else None
        }
    
    print(f"✅ 门店映射字典构建完成，共 {len(store_mapping)} 个门店")
    
    def get_store_field(store_name, field_name):
        if pd.isna(store_name) or str(store_name).strip() == '':
            return None
        store_key = str(store_name).strip()
        if store_key in store_mapping:
            return store_mapping[store_key].get(field_name)
        return None
    
    if not df_submission.empty and 'StoreName' in df_submission.columns:
        print("🔧 正在修正 Submission Facts 表的地理位置信息...")
        df_submission['Region'] = df_submission['StoreName'].apply(lambda x: get_store_field(x, 'Region'))
        df_submission['Country/Region'] = df_submission['StoreName'].apply(lambda x: get_store_field(x, 'Country'))
        df_submission['City'] = df_submission['StoreName'].apply(lambda x: get_store_field(x, 'City'))
        
    if not df_long.empty and 'StoreName' in df_long.columns:
        print("🔧 正在修正 fact_question_long 表的地理位置信息...")
        df_long['Region'] = df_long['StoreName'].apply(lambda x: get_store_field(x, 'Region'))
        df_long['Country/Region'] = df_long['StoreName'].apply(lambda x: get_store_field(x, 'Country'))
        df_long['City'] = df_long['StoreName'].apply(lambda x: get_store_field(x, 'City'))
    
    print("✅ 门店地理位置信息修正完成")
    return df_long, df_submission
