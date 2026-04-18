from flask import Flask, jsonify ,request
import os
from databricks.connect import DatabricksSession
from pyspark.sql.types import *
from delta.tables import DeltaTable
import time


app = Flask(__name__)

spark = None

dq_master_table = "workspace.dq_items.dq_master_v2"
purview_vertical_map_table = "workspace.dq_items.purview_vertical_map"
dq_execution_results = "workspace.dq_items.dq_execution_results"


def init_spark():
    global spark
    if spark is None:
        spark = DatabricksSession.builder.remote(
                host="https://dbc-32d63ff1-3673.cloud.databricks.com",
                token="dapi9996c758f9e5dd3ccb5184be16b2c624",
                serverless=True
            ).getOrCreate()
    return spark

def structured_data(spark, data):
    schema = StructType([
        StructField("governance_domain_name", StringType(), True),
        StructField("data_product_name", StringType(), True),
        StructField("data_asset_name", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("column_datatype", StringType(), True),
        StructField("dq_rule_id", StringType(), True),
        StructField("dq_rule_name", StringType(), True),
        StructField("dq_rule_description", StringType(), True),
        StructField("dq_rule_dimension", StringType(), True),
        StructField("dq_rule_type", StringType(), True),
        StructField("dq_rule_expression", StringType(), True),
        StructField("dq_rule_status", StringType(), True),
        StructField("rule_threshold_pct", LongType(), True),
        StructField("data_owner_name", StringType(), True),
        StructField("data_steward_name", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("rule_weightage", LongType(), True),
        StructField("is_current", BooleanType(), True),
        StructField("comments", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    return df

    
def data_parser(data):
    dq_df = []
    for blob in data:
        dq_data = {
            "governance_domain_name": blob.get("governance_domain_name"),
            "data_product_name": blob.get("data_product_name"),
            "data_asset_name": blob.get("data_asset_name"),
            "column_name": blob.get("column_name"),
            "column_datatype": blob.get("column_datatype"),
            "dq_rule_id": blob.get("dq_rule_id"),
            "dq_rule_name": blob.get("dq_rule_name"),
            "dq_rule_description": blob.get("dq_rule_description"),
            "dq_rule_dimension": blob.get("dq_rule_dimension"),
            "dq_rule_type": blob.get("dq_rule_type"),
            "dq_rule_expression": blob.get("dq_rule_expression"),
            "dq_rule_status": blob.get("dq_rule_status"),
            "rule_threshold_pct": blob.get("rule_threshold_pct"),
            "data_owner_name": blob.get("data_owner_name"),
            "data_steward_name": blob.get("data_steward_name"),
            "is_active":blob.get("isActive"),
            "rule_weightage": blob.get("rule_weightage"),
            "is_current":  blob.get("is_current"),
            "comments": blob.get("comments") 
        }

        dq_df.append(dq_data)

    return dq_df


def dq_master_updt(spark, spark_df, dq_master_table):

    delta_table = DeltaTable.forName(spark, dq_master_table)

    target = delta_table.alias("t")
    source = spark_df.alias("s")

    merge_condition = """
        t.dq_rule_id = s.dq_rule_id 
        AND t.is_current = true
    """

    change_condition = """
        NOT (t.dq_rule_status <=> s.dq_rule_status)
    """

    target.merge(
        source,
        merge_condition
    ) \
    .whenMatchedUpdate(
        condition=f"""
            {change_condition}
            AND lower(s.dq_rule_status) = 'approved'
        """,
        set={
            "dq_rule_status": "s.dq_rule_status",
            "is_active": "true"
        }
    ) \
    .whenMatchedUpdate(
        condition=f"""
            {change_condition}
            AND lower(s.dq_rule_status) <> 'approved'
        """,
        set={
            "dq_rule_status": "s.dq_rule_status"
        }
    ) \
    .execute()

def dq_data_blob(dq_master_table,purview_vertical_map_table,dq_execution_results, condition):
    df = spark.sql(f"""
    SELECT 
        a.vertical_name,
        b.governance_domain_name,
        b.data_product_name,
        b.data_asset_name,
        b.column_name,
        b.column_datatype,
        b.dq_rule_id,
        b.dq_rule_name,
        b.dq_rule_description,
        b.dq_rule_dimension,
        b.dq_rule_type,
        b.dq_rule_expression,
        b.dq_rule_status,
        b.rule_threshold_pct,
        b.data_owner_name,
        b.data_steward_name,
        b.is_active,
        b.rule_weightage,
        b.is_current,
        b.comments,
        c.score,
        c.dq_last_execution_date
    FROM {dq_master_table} b
    LEFT JOIN {purview_vertical_map_table} a ON b.dq_rule_id  = a.dq_rule_id 
    LEFT JOIN {dq_execution_results} c ON b.dq_rule_id  = c.dq_rule_id 
    WHERE {condition}
    """)
    return df


spark = init_spark()

@app.route("/purview", methods=["GET"])
def call_purview():
    try:
        condition = "b.is_current = true"
        time.sleep(3)
        df = dq_data_blob(dq_master_table,purview_vertical_map_table,dq_execution_results, condition)
        # result = [row.asDict(recursive=True) for row in df.collect()]
        result = df.limit(1000).toPandas().to_dict(orient="records")
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

        powerapps_data = data_parser(data)
        spark_df = structured_data(spark, powerapps_data)
        dq_master_updt(spark, spark_df, dq_master_table)

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

        powerapps_data = data_parser(data)
        spark_df = structured_data(spark, powerapps_data)
        dq_master_updt(spark, spark_df, dq_master_table)

        return {"status": "success", "count": len(data)}

    except Exception as e:
        app.logger.error(str(e))
        return {"status": "failed", "error": str(e)}, 500

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

        powerapps_data = data_parser(data)
        spark_df = structured_data(spark, powerapps_data)
        dq_master_updt(spark, spark_df, dq_master_table)

        return {"status": "success", "count": len(data)}

    except Exception as e:
        app.logger.error(str(e))
        return {"status": "failed", "error": str(e)}, 500

    except Exception as e:
        app.logger.error(str(e))
        return {"status": "failed", "error": str(e)}, 500

@app.route("/databricks", methods=["GET"])
def call_databricks():
    try:
        condition = "b.is_current = true AND b.dq_rule_status != 'new'"
        time.sleep(3)
        df = dq_data_blob(dq_master_table,purview_vertical_map_table,dq_execution_results, condition)
        # result = [row.asDict(recursive=True) for row in df.collect()]
        result = df.limit(1000).toPandas().to_dict(orient="records")
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