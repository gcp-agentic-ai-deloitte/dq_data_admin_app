from flask import Flask, jsonify
from azure.identity import DeviceCodeCredential, TokenCachePersistenceOptions
import requests
import os
import logging
import sys

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

# Reduce Azure SDK noise
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)

# =========================
# FLASK APP
# =========================
app = Flask(__name__)

# Ensure Flask logger uses same config
app.logger.handlers = logger.handlers
app.logger.setLevel(logging.INFO)
app.logger.propagate = True   # IMPORTANT

# =========================
# CONFIG
# =========================
PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"
GUID = "e1307a0a-2872-42ee-b9b3-b1f6f6f60000"

# =========================
# TOKEN CACHE
# =========================
cache_options = TokenCachePersistenceOptions(
    name="purview_token_cache"
)

# =========================
# DEVICE CODE CALLBACK
# =========================
def device_code_callback(verification_uri, user_code, expires_on):
    logger.info(f"Go to: {verification_uri}")
    logger.info(f"Enter code: {user_code}")
    logger.info(f"Expires on: {expires_on}")

# =========================
# GLOBAL CREDENTIAL
# =========================
credential = DeviceCodeCredential(
    prompt_callback=device_code_callback,
    cache_persistence_options=cache_options
)

# =========================
# INIT AUTH (GUNICORN SAFE)
# =========================
@app.before_first_request
def init_auth():
    try:
        logger.info("Triggering initial authentication...")
        credential.get_token(SCOPE)
        logger.info("Authentication initialized successfully")
    except Exception as e:
        logger.error(f"Auth initialization failed: {str(e)}")

# =========================
# GET HEADERS
# =========================
def get_headers():
    token = credential.get_token(SCOPE).token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

# =========================
# PURVIEW API
# =========================
@app.route("/purview", methods=["GET"])
def call_purview():
    try:
        url = f"{PURVIEW_ENDPOINT}/datamap/api/atlas/v2/entity/guid/{GUID}"

        response = requests.get(url, headers=get_headers())

        # Retry once if token expired
        if response.status_code == 401:
            logger.info("Token expired, retrying with fresh token...")
            response = requests.get(url, headers=get_headers())

        return jsonify({
            "status": response.status_code,
            "data": response.json()
        })

    except Exception as e:
        logger.error(f"API call failed: {str(e)}")
        return jsonify({
            "error": str(e),
            "message": "Authentication or API call failed"
        }), 500

# =========================
# LOCAL RUN (OPTIONAL)
# =========================
if __name__ == "__main__":
    logger.info("Running locally with Flask dev server...")
    app.run(debug=True)