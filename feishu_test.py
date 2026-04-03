import requests

# 1️⃣ Acquire Feishu access token
APP_ID = "cli_a945cc3d7938dcbc"
APP_SECRET = "UxoDHpOyrEAFW8EwUFmymhrPYghPGCFc"

url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
payload = {"app_id": APP_ID, "app_secret": APP_SECRET}

response = requests.post(url, json=payload)
data = response.json()

print("Full response:", data)

if "tenant_access_token" in data:
    token = data["tenant_access_token"]
    print("\n✅ Token acquired successfully!")
    print("Token:", token)
else:
    print("\n❌ Failed to get token")
    exit()  # stop script if token fails



# 2️⃣ Download data from Bitable
APP_TOKEN = "D3edbzTbdas2PfsjcsQcK8JtnCd"
TABLE_ID = "tblBVQ6tubyIqrOl"

url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/records"
headers = {"Authorization": f"Bearer {token}"}

response = requests.get(url, headers=headers)
records = response.json()

print(records)