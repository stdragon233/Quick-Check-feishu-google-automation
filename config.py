import os

# =============================================================================
# 🔐 1. 凭证与密钥中心 (优先读取 GitHub 环境变量，本地留有兜底)
# =============================================================================

# 飞书 API 凭证
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a945cc3d7938dcbc")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "UxoDHpOyrEAFW8EwUFmymhrPYghPGCFc")
FEISHU_APP_TOKEN = "D3edbzTbdas2PfsjcsQcK8JtnCd"

# Google Sheets 凭证 (GitHub Actions 会作为环境变量以 JSON 字符串形式传入)
# 本地测试时，如果没配环境变量，可以指定你的本地路径作为备份读取
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS")
LOCAL_GOOGLE_CONFIG_PATH = "secret/gsp-config.json"

# 你要写入的目标 Google Sheet 的名字或它的 Spreadsheet ID
TARGET_GOOGLE_SHEET_NAME = "XPENG Quick Check Dash"  # 👈 可以改成你真实的 Google Sheet 名字


# =============================================================================
# 🚫 2. 飞书多维表格下载黑名单
# =============================================================================
EXCLUDE_TABLES = [
    "fact_question_long",          
    "Submission Facts",            
    "Attachment Sync Collection",  
    "France Zone Manual Match",
    "Target Coverage",
    "Sweden Email Collection",
    "Submission Records",
    "Store Coverage Count",
    "Count Down"
]



# =============================================================================
# 📊 3. 核心清洗映射配置中心 (从你的 Colab 完美搬迁)
# =============================================================================

# 全局问题大类映射
VERSION_MAPS = {
    "Quick Check": {
        "1": "Sales",
        "2": "Delivery",
        "3": "Aftersales",
        "4": "Marketing&UserOperation",
        "5": "StaffDemeanor"
    },
    "Regional Check 2.0": {
        "1": "Sales&Delivery",
        "2": "Aftersales",
        "3": "User Experience"
    }
}

# 门店表单注册中心（精准控制每一个国家的结构与分类大类）
SHEET_CONFIG = {
    # 法国是 nested 结构，但沿用老版 Quick Check 分类
    "France Quick Check 2.0":       {"structure": "nested", "category": "Quick Check"},

    # 新版 Regional Check 阵营：全部是 nested 结构 + 新版 Regional Check 2.0 分类
    "Sweden Regional Check":        {"structure": "nested", "category": "Regional Check 2.0"},
    "Norway Regional Check":        {"structure": "nested", "category": "Regional Check 2.0"},
    "Global Regional Check":        {"structure": "nested", "category": "Regional Check 2.0"},
    "Denmark Regional Check":       {"structure": "nested", "category": "Regional Check 2.0"},
    "Netherlands Regional Check":   {"structure": "nested", "category": "Regional Check 2.0"},
    "Thailand Regional Check":      {"structure": "nested", "category": "Regional Check 2.0"},
    "Indonesia Regional Check V2":  {"structure": "nested", "category": "Regional Check 2.0"},
    "Hong Kong Regional Check":     {"structure": "nested", "category": "Regional Check 2.0"},
    "MEA Regional Check":           {"structure": "nested", "category": "Regional Check 2.0"},

    # 💡 提示：任何未在这里列出的传统 Sheet，
    # 自动激活下游循环中的【兜底机制】，默认以 simple 结构 + Quick Check 分类运行。
}
