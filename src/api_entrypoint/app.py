from flask import Flask, jsonify ,request
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from azure.identity import DeviceCodeCredential, TokenCachePersistenceOptions
from databricks.connect import DatabricksSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp, lit

from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import logging


logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)


app = Flask(__name__)

# Enable logging
logging.basicConfig(level=logging.INFO)


def init_spark():
    return DatabricksSession.builder.remote(
            host="https://dbc-32d63ff1-3673.cloud.databricks.com",
            token="dapi9996c758f9e5dd3ccb5184be16b2c624",
            serverless=True
        ).getOrCreate()


table_path = "workspace.dq_items.dq_rule_master_test"

PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"



abu_dhabi_tz = ZoneInfo("Asia/Dubai")
formatted = datetime.now(abu_dhabi_tz)

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

spark = init_spark()


def is_table_empty(spark, table_name):
    df = spark.table(table_name)
    return df.limit(1).count() == 0

# def val_parser(data):

#     schema = StructType([
#                 StructField("businessDomainName", StringType(), True),
#                 StructField("dataProductName", StringType(), True),
#                 StructField("assetName", StringType(), True),
#                 StructField("dqName", StringType(), True),
#                 StructField("id", StringType(), True),
#                 StructField("description", StringType(), True),
#                 StructField("SQLcondition", StringType(), True),
#                 StructField("columns", StringType(), True),
#                 StructField("dimension", StringType(), True),
#                 StructField("threshold", StringType(), True),
#                 StructField("purviewStatus", StringType(), True),
#                 StructField("createdAtPurview", StringType(), True),
#                 StructField("lastModifiedAtPurview", StringType(), True),
#                 StructField("validationStatus", StringType(), True),
#                 StructField("validationDateTime", StringType(), True)
#             ])
#     dq_df = []

#     for blob in data:
#         # if blob.get("status", "").lower() != "active":
#         #     continue
#         dq_data = {
#             "businessDomainName": blob.get("businessDomainName"),
#             "dataProductName": blob.get("dataProductName"),
#             "assetName": blob.get("asset_name"),
#             "dqName": blob.get("name"),
#             "id": blob.get("id"),
#             "description": blob.get("description"),
#             "SQLcondition": blob.get("SQLcondition"),
#             "columns": blob.get("columns"),
#             "dimension": blob.get("dimension"),
#             "threshold": blob.get("threshold"),
#             "purviewStatus": blob.get("status"),
#             "createdAtPurview": blob.get("createdAt"),
#             "lastModifiedAtPurview": blob.get("lastModifiedAt"),
#             "validationStatus": blob.get("validationStatus"),
#             "validationDateTime": str(formatted)
#         }
#         dq_df.append(dq_data)

#     spark_df = spark.createDataFrame(dq_df, schema=schema)
#     spark_df.write \
#     .format("delta") \
#     .mode("append") \
#     .saveAsTable(table_path)    


def purview_dq_data_parser( spark, data, businessDomainName, dataProductName, asset_name):

    schema = StructType([
        StructField("businessDomainName", StringType(), True),
        StructField("dataProductName", StringType(), True),
        StructField("assetName", StringType(), True),
        StructField("dqName", StringType(), True),
        StructField("id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("SQLcondition", StringType(), True),
        StructField("columns", StringType(), True),
        StructField("dimension", StringType(), True),
        StructField("threshold", IntegerType(), True),  
        StructField("weight", IntegerType(), True),      
        StructField("purviewStatus", StringType(), True),
        StructField("createdAtPurview", StringType(), True),   
        StructField("lastModifiedAtPurview", StringType(), True), 
        StructField("status", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("isActive", StringType(), True),
        StructField("loadDateTime", TimestampType(), True), 
        StructField("startDateTime", TimestampType(), True), 
        StructField("endDateTime", TimestampType(), True), 

    ])

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
            "weight": 80,
            "purviewStatus": blob.get("status"),
            "createdAtPurview": blob.get("createdAt"),
            "lastModifiedAtPurview": blob.get("lastModifiedAt"),
            "status": "new",
            "comment": None,
            "isActive": "Y",
            "loadDateTime": None,
            "startDateTime": None,
            "endDateTime": None
        }

        dq_df.append(dq_data)

    spark_df = spark.createDataFrame(dq_df, schema=schema)
    return spark_df

def dq_master_loader(spark, spark_df, table_path):
    ts = from_utc_timestamp(current_timestamp(), "Asia/Dubai")
            
    spark_df = spark_df.withColumn(
        "loadDateTime",
        ts
    ).withColumn(
        "startDateTime",
        ts
    ).withColumn(
        "endDateTime",
        lit(None).cast("timestamp")
    )

    if is_table_empty(spark, table_path):
            spark_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(table_path)
            spark.catalog.refreshTable(table_path)
            df =  spark.table(table_path)
            active_df = df.filter(df.isActive == "Y")
            result = active_df.limit(1000).toPandas().to_dict(orient="records")
            return result
                    
    else:
        
        delta_table = DeltaTable.forName(spark, table_path)

        # Alias
        target = delta_table.alias("t")
        source = spark_df.alias("s")

        # Condition for matching active records
        merge_condition = "t.id = s.id AND t.isActive = 'Y'"

        # Columns to check for change
        change_condition = """
           NOT( t.description <=> s.description AND
            t.SQLcondition <=> s.SQLcondition AND
            t.dimension <=> s.dimension AND
            t.purviewStatus <=> s.purviewStatus)
        """

        # Step 1: Expire old records where change detected
        target.merge(
            source,
            merge_condition
        ).whenMatchedUpdate(
            condition=change_condition,
            set={
                "isActive": "'N'",
                "endDateTime": "from_utc_timestamp(current_timestamp(), 'Asia/Dubai')"
            }
        ).execute()

        # Step 2: Insert new records (new OR changed)
        target.merge(
            source,
            merge_condition
        ).whenNotMatchedInsert(
            values={
                "businessDomainName": "s.businessDomainName",
                "dataProductName": "s.dataProductName",
                "assetName": "s.assetName",
                "dqName": "s.dqName",
                "id": "s.id",
                "description": "s.description",
                "SQLcondition": "s.SQLcondition",
                "`columns`": "s.columns",
                "dimension": "s.dimension",
                "threshold": "s.threshold",
                "weight": "s.weight",
                "purviewStatus": "s.purviewStatus",
                "createdAtPurview": "s.createdAtPurview",
                "lastModifiedAtPurview": "s.lastModifiedAtPurview",
                "status": "s.status",
                "comment": "s.comment",
                "isActive": "s.isActive",
                "loadDateTime": "s.loadDateTime",
                "startDateTime":"s.startDateTime",
                "endDateTime": "s.endDateTime"
            }
        ).execute()
        spark.catalog.refreshTable(table_path)
        df =  spark.table(table_path)
        active_df = df.filter(df.isActive == "Y")
        result = active_df.limit(1000).toPandas().to_dict(orient="records")
        return result

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
                try:
                    spark_df = purview_dq_data_parser(spark, data, businessDomainName, dataProductName, asset_name)
                    result = dq_master_loader(spark, spark_df, table_path)
                    return jsonify(result)
                except Exception as e:
                    return {
                        "status": "failed",
                        "error": str(e)
                    }

    except Exception as e:
        return jsonify({
            "error": str(e),
            "message": "Authentication or API call failed"
        }), 500


# @app.route("/dqcheck/update", methods=["POST"])
# def dqcheck_update():
#     data = request.json
#     try:
#         val_parser(data)
#         return {"status": "success"}
#     except:
#         return {"status": "failes"}


# =========================
# APP STARTUP
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # <-- important
    app.run(host="0.0.0.0", port=port)