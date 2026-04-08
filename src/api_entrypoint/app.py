from flask import Flask, jsonify
from azure.identity import DeviceCodeCredential
import requests
from datetime import datetime

app = Flask(__name__)

PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"
GUID = "e1307a0a-2872-42ee-b9b3-b1f6f6f60000"

# Store login info globally (POC only)
login_info = {}
headers = {}

# =========================
# DEVICE CODE CALLBACK
# =========================
def device_code_callback(verification_uri, user_code, expires_on):
    global login_info

    login_info = {
        "verification_uri": verification_uri,
        "user_code": user_code,
        "expires_on_epoch": expires_on,
    }

# Create credential
# credential = DeviceCodeCredential(prompt_callback=device_code_callback)

# =========================
# LOGIN ROUTE
# =========================
@app.route("/login", methods=["GET"])
def login():
    try:
        # Create credential
        DeviceCodeCredential(prompt_callback=device_code_callback)

        if not login_info:
            return jsonify({
                "error": "Device code not generated",
                "hint": "Callback not triggered. Check logs."
            }), 500

        return jsonify({
            "status": "login_required",
            "login": login_info,
            "note": "Go to verification_uri and enter user_code"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================
# MAIN API
# =========================
@app.route("/purview", methods=["GET"])
def call_purview():
    try:
        token = credential.get_token(SCOPE).token

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        url = f"{PURVIEW_ENDPOINT}/datamap/api/atlas/v2/entity/guid/{GUID}"

        response = requests.get(url, headers=headers)

        return jsonify({
            "status": response.status_code,
            "data": response.json()
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "message": "Authenticate first using /login",
            "login_hint": login_info
        }), 500

# =========================
# HEALTH CHECK
# =========================
@app.route("/", methods=["GET"])
def home():
    return jsonify({"message": "API is running"})

# =========================
# RUN
# =========================
if __name__ == "__main__":
    app.run(debug=True)