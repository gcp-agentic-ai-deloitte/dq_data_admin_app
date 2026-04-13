from flask import Flask, jsonify ,request
from datetime import datetime
from zoneinfo import ZoneInfo
import os
from azure.identity import DeviceCodeCredential, TokenCachePersistenceOptions
from databricks.connect import DatabricksSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, from_utc_timestamp, lit,to_timestamp
import time
import json
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


table_path = "workspace.dq_items.dq_rule_status_log"
target_path = "workspace.dq_items.dq_master"
PURVIEW_ENDPOINT = "https://adgov-datagovernance-purview.purview.azure.com"
SCOPE = "https://purview.azure.net/.default"
abu_dhabi_tz = ZoneInfo("Asia/Dubai")
formatted = datetime.now(abu_dhabi_tz)
auth_initialized = False

# =========================
# TOKEN CACHE (PERSISTENT)
# =========================
cache_options = TokenCachePersistenceOptions(
    name="purview_token_cache",
    allow_unencrypted_storage=True 
)

spark = init_spark()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def read_json_file():
    file_path =  os.path.join(BASE_DIR, "governance_domains", "service_now_foundational.json")

    with open(file_path, "r") as f:
        data = json.load(f)

    return data

def is_table_empty(spark, table_name):
    df = spark.table(table_name)
    return df.limit(1).count() == 0

def parse_datetime(val):
    if val is None:
        return None
    try:
        return datetime.strptime(val, "%a, %d %b %Y %H:%M:%S %Z")
    except:
        return None
    

def structured_data(spark, data):
    schema = StructType([
        StructField("businessDomainName", StringType(), True),
        StructField("governance_domain_id", StringType(), True),
        StructField("dataProductName", StringType(), True),
        StructField("data_product_id", StringType(), True),
        StructField("assetName", StringType(), True),
        StructField("data_asset_id", StringType(), True),
        StructField("asset_qualified_name", StringType(), True),
        StructField("dqName", StringType(), True),
        StructField("id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("ruleType", StringType(), True),
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
        StructField("data_owner_name", StringType(), True),
        StructField("data_steward_name", StringType(), True),
        StructField("isActive", StringType(), True),
        StructField("loadDateTime", TimestampType(), True), 
        StructField("startDateTime", TimestampType(), True), 
        StructField("endDateTime", TimestampType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    return df

def domain_extractor(domain_data, PURVIEW_ENDPOINT):
    source_data = []
    for data in domain_data:
        governance_domain_id = data.get("governance_domain_id")
        governance_domain_name = data.get("governance_domain_name")
        data_product_id = data.get("data_product_id")
        data_product_name = data.get("data_product_name")
        data_asset_id = data.get("data_asset_id")
        data_asset_name = data.get("data_asset_name")
        asset_qualified_name = data.get("asset_qualified_name")

        try:
            url = f"{PURVIEW_ENDPOINT}/datagovernance/quality/business-domains/{governance_domain_id}/data-products/{data_product_id}/data-assets/{data_asset_id}/rules?api-version=2025-09-01-preview"

            response = requests.get(url, headers=get_headers())
            data = response.json()
            for blob in data:
                dq_data = {
                    "businessDomainName": governance_domain_name,
                    "governance_domain_id": governance_domain_id,
                    "dataProductName": data_product_name,
                    "data_product_id": data_product_id,
                    "assetName": data_asset_name,
                    "data_asset_id":data_asset_id,
                    "asset_qualified_name":asset_qualified_name,
                    "dqName": blob.get("name"),
                    "id": blob.get("id"),
                    "description": blob.get("description"),
                    "ruleType":blob.get("type"),
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
                    "data_owner_name": None,
                    "data_steward_name": None,
                    "isActive": "Y",
                    "loadDateTime": None,
                    "startDateTime": None,
                    "endDateTime": None
                }

                source_data.append(dq_data)
        except Exception as e:
            print(f"Error: {e}")
    return source_data  


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
                "governance_domain_id": "s.governance_domain_id",
                "dataProductName": "s.dataProductName",
                "data_product_id": "s.data_product_id",
                "assetName": "s.assetName",
                "data_asset_id": "s.data_asset_id",
                "asset_qualified_name" : "s.asset_qualified_name",
                "dqName": "s.dqName",
                "id": "s.id",
                "description": "s.description",
                "ruleType":"s.ruleType",
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
                "data_owner_name": "s.data_owner_name",
                "data_steward_name": "s.data_steward_name",
                "isActive": "s.isActive",
                "loadDateTime": "s.loadDateTime",
                "startDateTime":"s.startDateTime",
                "endDateTime": "s.endDateTime"
            }
        ).execute()
    
def powerapp_dq_data_parser(data):
    dq_df = []
    for blob in data:
        dq_data = {
            "businessDomainName": blob.get("businessDomainName"),
            "governance_domain_id": blob.get("governance_domain_id"),
            "dataProductName": blob.get("dataProductName"),
            "data_product_id": blob.get("data_product_id"),
            "assetName": blob.get("assetName"),
            "data_asset_id": blob.get("data_asset_id"),
            "asset_qualified_name" : blob.get("asset_qualified_name"),
            "dqName": blob.get("dqName"),
            "id": blob.get("id"),
            "description": blob.get("description"),
            "ruleType": blob.get("ruleType"),
            "SQLcondition": blob.get("SQLcondition"),
            "columns": blob.get("columns"),
            "dimension": blob.get("dimension"),
            "threshold": blob.get("threshold"),
            "weight": blob.get("weight"),
            "purviewStatus": blob.get("purviewStatus"),
            "createdAtPurview": blob.get("createdAtPurview"),
            "lastModifiedAtPurview": blob.get("lastModifiedAtPurview"),
            "status": blob.get("status"),
            "comment": blob.get("comment"),
            "data_owner_name": blob.get("data_owner_name"),
            "data_steward_name": blob.get("data_steward_name"),
            "isActive": blob.get("isActive"),
            "loadDateTime": parse_datetime(blob.get("loadDateTime")),
            "startDateTime": parse_datetime(blob.get("startDateTime")),
            "endDateTime": parse_datetime(blob.get("endDateTime"))
        }

        dq_df.append(dq_data)

    return dq_df

def dq_rule_log_updater(spark, spark_df, table_path):
        
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
            t.purviewStatus <=> s.purviewStatus AND
            t.status <=> s.status)
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
                "governance_domain_id": "s.governance_domain_id",
                "dataProductName": "s.dataProductName",
                "data_product_id": "s.data_product_id",
                "assetName": "s.assetName",
                "data_asset_id": "s.data_asset_id",
                "asset_qualified_name" : "s.asset_qualified_name",
                "dqName": "s.dqName",
                "id": "s.id",
                "description": "s.description",
                "ruleType":"s.ruleType",
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
                "data_owner_name": "s.data_owner_name",
                "data_steward_name": "s.data_steward_name",
                "isActive": "s.isActive",
                "loadDateTime": "s.loadDateTime",
                "startDateTime":"s.startDateTime",
                "endDateTime": "s.endDateTime"
            }
        ).execute()


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


def load_dq_master(spark, target_path, table_path):
    spark.sql(
        f"""TRUNCATE TABLE {target_path}
        """
    )
    spark.sql(f"""
            INSERT INTO {target_path} (
            dq_master_id,
            governance_domain_name,
            data_product_id,
            data_product_name,
            data_asset_id,
            data_asset_name,
            asset_qualified_name,
            column_name,
            column_datatype,
            dq_rule_id,
            dq_rule_name,
            dq_rule_description,
            dq_rule_dimension,
            dq_rule_type,
            dq_rule_expression,
            dq_rule_status,
            rule_threshold_pct,
            data_owner_name,
            data_steward_name,
            is_active,
            rule_weightage,
            hash_diff,
            effective_start_ts,
            effective_end_ts,
            is_current
            )
            SELECT
            ROW_NUMBER() OVER (ORDER BY id) AS dq_master_id,
            CAST(businessDomainName AS STRING),
            CAST(data_product_id AS STRING),
            CAST(dataProductName AS STRING),
            CAST(data_asset_id AS STRING),
            CAST(assetName AS STRING),
            CAST(asset_qualified_name AS STRING),
            CAST(columns AS STRING) AS column_name,            
            NULL AS column_datatype,
            CAST(id AS STRING),
            CAST(dqName AS STRING),
            CAST(description AS STRING),
            CAST(dimension AS STRING),
            CAST(ruleType AS STRING) dq_rule_type,
            CAST(SQLcondition AS STRING),
            CAST(status AS STRING),
            CAST(threshold AS BIGINT),
            CAST(data_owner_name AS STRING),
            CAST(data_steward_name AS STRING),
            CASE WHEN isActive = 'Y' THEN TRUE ELSE FALSE END, 
            CAST(weight AS BIGINT),
            NULL,
            CURRENT_TIMESTAMP(),
            NULL,
            TRUE
            FROM {table_path}
            WHERE status = 'approved'
            AND isActive = 'Y'
    """)

# =========================
# PURVIEW API CALL
# =========================
@app.route("/sync", methods=["GET"])
def sync_purview():
    try:
        time.sleep(3)
        domain_data = read_json_file()
        data_extract = domain_extractor(domain_data, PURVIEW_ENDPOINT)
        domain_df = structured_data(spark, data_extract)
        dq_master_loader(spark, domain_df, table_path)
        return {"status":"purview synced with databricks successfully!", "check_count":len(data_extract)}
    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }



@app.route("/purview", methods=["GET"])
def call_purview():
    try:
        spark.sql("SELECT 1").collect()

        # Allow commit propagation
        time.sleep(3)

        # Ensure table ready
        spark.table(table_path).limit(1).collect()

        # Actual query
        df = spark.table(table_path).filter("isActive = 'Y'").limit(100)

        result = [row.asDict(recursive=True) for row in df.collect()]

        return jsonify(result)

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }


@app.route("/dqcheck/owner", methods=["POST"])
def dqcheck_update():
    try:
        data = request.get_json(force=True)

        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            return {"status": "failed", "error": "Empty payload"}, 400

        powerapps_data = powerapp_dq_data_parser(data)
        spark_df = structured_data(spark, powerapps_data)
        # Fix timestamps
        spark_df = spark_df.withColumn(
            "loadDateTime",
            to_timestamp("loadDateTime", "EEE, dd MMM yyyy HH:mm:ss z")
        ).withColumn(
            "startDateTime",
            to_timestamp("startDateTime", "EEE, dd MMM yyyy HH:mm:ss z")
        )

        dq_rule_log_updater(spark, spark_df, table_path)

        return {"status": "success", "count": len(data)}

    except Exception as e:
        app.logger.error(str(e))
        return {"status": "failed", "error": str(e)}, 500
    


@app.route("/approve", methods=["POST"])
def dqcheck_approve():
    try:
        data = request.get_json(force=True)

        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            return {"status": "failed", "error": "Empty payload"}, 400

        powerapps_data = powerapp_dq_data_parser(data)
        spark_df = structured_data(spark, powerapps_data)
        # Fix timestamps
        spark_df = spark_df.withColumn(
            "loadDateTime",
            to_timestamp("loadDateTime", "EEE, dd MMM yyyy HH:mm:ss z")
        ).withColumn(
            "startDateTime",
            to_timestamp("startDateTime", "EEE, dd MMM yyyy HH:mm:ss z")
        )

        dq_rule_log_updater(spark, spark_df, table_path)
        load_dq_master(spark, target_path, table_path)

        return {"status": "success", "count": len(data)}

    except Exception as e:
        app.logger.error(str(e))
        return {"status": "failed", "error": str(e)}, 500
    

@app.route("/reject", methods=["POST"])
def dqcheck_reject():
    try:
        data = request.get_json(force=True)

        if isinstance(data, dict):
            data = data.get("data", [])

        if not data:
            return {"status": "failed", "error": "Empty payload"}, 400

        powerapps_data = powerapp_dq_data_parser(data)
        spark_df = structured_data(spark, powerapps_data)
        # Fix timestamps
        spark_df = spark_df.withColumn(
            "loadDateTime",
            to_timestamp("loadDateTime", "EEE, dd MMM yyyy HH:mm:ss z")
        ).withColumn(
            "startDateTime",
            to_timestamp("startDateTime", "EEE, dd MMM yyyy HH:mm:ss z")
        )

        dq_rule_log_updater(spark, spark_df, table_path)

        return {"status": "success", "count": len(data)}

    except Exception as e:
        app.logger.error(str(e))
        return {"status": "failed", "error": str(e)}, 500

@app.route("/databricks", methods=["GET"])
def call_databricks():
    try:
        spark.sql("SELECT 1").collect()

        # Allow commit propagation
        time.sleep(3)

        # Ensure table ready
        spark.table(table_path).limit(1).collect()

        # Actual query
        df = spark.table(table_path).filter("isActive = 'Y' AND status != 'new'").limit(100)

        result = [row.asDict(recursive=True) for row in df.collect()]

        return jsonify(result)

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e)
        }


# =========================
# APP STARTUP
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))  # <-- important
    app.run(host="0.0.0.0", port=port)