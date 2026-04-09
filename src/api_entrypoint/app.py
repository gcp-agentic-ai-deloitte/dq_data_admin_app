from flask import Flask, jsonify
from datetime import datetime
from azure.identity import DeviceCodeCredential, TokenCachePersistenceOptions
import requests
import logging


logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)


app = Flask(__name__)

# Enable logging
logging.basicConfig(level=logging.INFO)


PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"


businessDomainId = "17c856d9-01d1-4ed5-aa73-f3bdecabdd93"
businessDomainName = "Cybersecurity - Foundational"
dataProductId = "79b00c61-b8c6-417d-b269-b49d703b4499"
dataProductName = "Cybersecurity - Consumption Layer"
asset_id = "d762170f-dfc8-4c10-aede-a49021bda745"
asset_name = "gld_cybersec_fact_machine_vulnerabilities_snapshot"


auth_initialized = False

# =========================
# TOKEN CACHE (PERSISTENT)
# =========================
cache_options = TokenCachePersistenceOptions(
    name="purview_token_cache",
    allow_unencrypted_storage=True 
)



def data_parser(data, businessDomainName, dataProductName, asset_name):
    dq_df = []

    for blob in data:
        # if blob.get("status", "").lower() != "active":
        #     continue
        dq_data = {
            "businessDomainName": businessDomainName,
            "dataProductName": dataProductName,
            "assetName": asset_name,
            "dqName": blob.get("name"),
            "id": blob.get("id"),
            "description": blob.get("description"),
            "SQLcondition": blob.get("typeProperties", {}).get("condition"),
            "columns": ", ".join(
                                    [x.get("value") for x in blob.get("typeProperties", {}).get("columns", [])]
                                ),
            "dimension": blob.get("dimension"),
            "threshold": 80,
            "purviewStatus": blob.get("status"),
            "createdAtPurview": blob.get("createdAt"),
            "lastModifiedAtPurview": blob.get("lastModifiedAt"),
            "approvalStatus": "Pending",
            "isActive":"N",
            "approvalDateTime":datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        dq_df.append(dq_data)
    return dq_df    

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

# app.logger.info("Triggering initial authentication...")
# credential.get_token(SCOPE)  # triggers device login once
# app.logger.info("Running the app...")

def init_auth():
    global auth_initialized
    if not auth_initialized:
        app.logger.info("Triggering authentication...")
        credential.get_token(SCOPE)
        auth_initialized = True

init_auth()
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
        url = f"{PURVIEW_ENDPOINT}/datagovernance/quality/business-domains/{businessDomainId}/data-products/{dataProductId}/data-assets/{asset_id}/rules?api-version=2025-09-01-preview"

        response = requests.get(url, headers=get_headers())

        # Retry once if token expired
        if response.status_code == 401:
            app.logger.info("Token expired, retrying with fresh token...")
            response = requests.get(url, headers=get_headers())


        if response.status_code == 200:
            
                data = response.json()
                
                dq_df = data_parser(data, businessDomainName, dataProductName, asset_name)

  

        return jsonify(dq_df)

    except Exception as e:
        return jsonify({
            "error": str(e),
            "message": "Authentication or API call failed"
        }), 500

# =========================
# APP STARTUP
# =========================
if __name__ == "__main__":
    app.run(debug=True)