# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c",
# META       "default_lakehouse_name": "FabricLH",
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Export-CSV
# 
# This notebook exports data from a SQL source (table, view, or stored procedure), transforms it into a Spark DataFrame, and exports the result as a CSV file to a specified path in the Fabric Lakehouse. 
# It supports dynamic SQL query generation, automatic type handling for null columns, and includes configurable export options such as header inclusion and escape character settings. 


# PARAMETERS CELL ********************

SourceSettings ='{"Object":"FabricLH.dbo.accountrangerules"}'
TargetSettings ='{"Directory": "export/csv", "File":"AccountRangeRules.csv"}'
SourceConnectionSettings=None
TargetConnectionSettings=None
ActivitySettings=None
LineageKey = 0


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import re
import struct
import pyodbc
import pandas as pd
import json
import sempy.fabric as fabric
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, NullType
from builtin.sql_connection_helper import create_engine

SourceSettings = SourceSettings or '{}'
TargetSettings = TargetSettings or '{}'

source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)

if "header" not in target_settings:
    target_settings["header"] = True

if "escape" not in target_settings:
    target_settings["escape"] = '"'

target_directory = target_settings["Directory"]
target_file = target_settings["File"]


FILES_PREFIX = "Files"
LAKEHOUSE_DEFAULT_PREFIX = "/lakehouse/default/"
if not target_directory.startswith(FILES_PREFIX):
   target_directory = os.path.join(FILES_PREFIX, target_directory).replace("\\", "/")

target_path = os.path.join(target_directory, target_file)

temp_target_path = os.path.join(target_directory, f"_{target_file}")

del target_settings["Directory"]
del target_settings["File"]

tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
sql_end_point= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point}"
pattern = '[ ,;{}()\n\t/=]'

# List Datasets from meta data
engine = create_engine(connection_string)



source_object = source_settings.get("Object")

if "usp_" in source_object.lower() and not source_object.lower().startswith("exec"): # sproc without EXEC
    query = f"EXEC {source_object}"
elif " " not in source_object:
    query = f"SELECT * FROM {source_object}"
else:
    query = source_object


with engine.connect() as alchemy_connection:
    df = pd.read_sql_query(query, alchemy_connection)

spark_df = spark.createDataFrame(df)

for column in spark_df.columns:
    if spark_df.schema[column].dataType == NullType() and spark_df.select(col(column)).first()[0] is None:
        spark_df = spark_df.withColumn(column, col(column).cast(StringType()))


spark_df.repartition(1)\
    .write.format("com.databricks.spark.csv")\
    .mode("overwrite")\
    .options(**target_settings)\
    .save(temp_target_path)

for file in mssparkutils.fs.ls(temp_target_path):
    if file.name.endswith(".csv"):
        mssparkutils.fs.mv(os.path.join(temp_target_path, file.name), target_path, True, True)
        break

mssparkutils.fs.rm(temp_target_path, True)

print(f"CSV saved to {target_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
