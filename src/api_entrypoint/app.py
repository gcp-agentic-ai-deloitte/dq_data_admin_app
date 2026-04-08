from flask import Flask, jsonify
from azure.identity import DeviceCodeCredential, TokenCachePersistenceOptions
import requests
import os
import logging
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)
app = Flask(__name__)

# Enable logging
logging.basicConfig(level=logging.INFO)

PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"
GUID = "e1307a0a-2872-42ee-b9b3-b1f6f6f60000"

# =========================
# TOKEN CACHE (PERSISTENT)
# =========================
cache_options = TokenCachePersistenceOptions(
    name="purview_token_cache"
)

# =========================
# DEVICE CODE CALLBACK
# =========================
def device_code_callback(verification_uri, user_code, expires_on):
    app.logger.info(f"Go to: {verification_uri}")
    app.logger.info(f"Enter code: {user_code}")
    app.logger.info(f"Expires on: {expires_on}")

# =========================
# GLOBAL CREDENTIAL (IMPORTANT)
# =========================
credential = DeviceCodeCredential(
    prompt_callback=device_code_callback,
    cache_persistence_options=cache_options
)

# =========================
# GET AUTH HEADERS (AUTO CACHE + REFRESH)
# =========================
def get_headers():
    token = credential.get_token(SCOPE).token

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

# =========================
# PURVIEW API CALL
# =========================
@app.route("/purview", methods=["GET"])
def call_purview():
    try:
        url = f"{PURVIEW_ENDPOINT}/datamap/api/atlas/v2/entity/guid/{GUID}"

        response = requests.get(url, headers=get_headers())

        # Retry once if token expired
        if response.status_code == 401:
            app.logger.info("Token expired, retrying with fresh token...")
            response = requests.get(url, headers=get_headers())

        return jsonify({
            "status": response.status_code,
            "data": response.json()
        })

    except Exception as e:
        return jsonify({
            "error": str(e),
            "message": "Authentication or API call failed"
        }), 500

# =========================
# APP STARTUP
# =========================
if __name__ == "__main__":
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        app.logger.info("Triggering initial authentication...")
        credential.get_token(SCOPE)  # triggers device login once
        app.logger.info("Running the app...")
    app.run(debug=True)