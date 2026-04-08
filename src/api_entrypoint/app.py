from flask import Flask, jsonify
from azure.identity import DeviceCodeCredential
import requests

app = Flask(__name__)

PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"
guid = "e1307a0a-2872-42ee-b9b3-b1f6f6f60000"
# =========================
# DEVICE CODE CALLBACK
# =========================
def device_code_callback(verification_uri, user_code, expires_on):
    print("\n🔐 LOGIN REQUIRED")
    print(f"Go to: {verification_uri}")
    print(f"Enter code: {user_code}")
    print(f"Expires at: {expires_on}")
    print("=========================\n")

# Create credential (global so token can be reused)
credential = DeviceCodeCredential(prompt_callback=device_code_callback)

# =========================
# GET TOKEN
# =========================
def get_access_token():
    token = credential.get_token(SCOPE)
    return token.token

# =========================
# ROUTE
# =========================
@app.route("/purview", methods=["GET"])
def call_purview():
    try:
        token = get_access_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        url = f"{PURVIEW_ENDPOINT}/datamap/api/atlas/v2/entity/guid/{guid}"

        response = requests.get(url, headers=headers)

        return jsonify({
            "status": response.status_code,
            "data": response.json()
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================
# RUN
# =========================
if __name__ == "__main__":
    app.run(debug=True)