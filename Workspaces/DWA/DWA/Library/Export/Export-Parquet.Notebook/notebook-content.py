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
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Export-Parquet
# 
# This notebook extracts data from a SQL source—such as a table, view, stored procedure, or custom query—and exports it as a Parquet file to a designated Fabric Lakehouse directory. It builds a dynamic SQL query based on the provided source settings, handles null column types to ensure compatibility, and writes the data using Spark with configurable export options. 

# PARAMETERS CELL ********************

SourceSettings ='{"Query":"SELECT * FROM FabricDW.aw.DimDate"}'
TargetSettings ='{"Directory": "Export/parquet", "File": "DimDate.parquet"}'
SourceConnectionSettings = None
TargetConnectionSettings = None
ActivitySettings = None
LineageKey = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run SQL-Connection-Shared-Functions

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

# Set your connection settings
table_name = source_settings.get("TableName") or source_settings.get("Table")
stored_procedure_name = source_settings.get("StoredProcedureName") or source_settings.get("StoredProcedure")
view_name = source_settings.get("ViewName") or source_settings.get("View")
query = source_settings.get("Query")

if query:
    pass # query passed
elif table_name or view_name:
    query = f"SELECT * FROM {table_name or view_name}"
elif stored_procedure_name:
    query = f"EXEC {stored_procedure_name}"
else:
    raise Exception("No valid source specified (Table, Stored Procedure, Query or View).")
    
with engine.connect() as alchemy_connection:
    df = pd.read_sql_query(query, alchemy_connection)

spark_df = spark.createDataFrame(df)

for column in spark_df.columns:
    if spark_df.schema[column].dataType == NullType() and spark_df.select(col(column)).first()[0] is None:
        spark_df = spark_df.withColumn(column, col(column).cast(StringType()))
        
spark_df.repartition(1)\
    .write.format("parquet")\
    .mode("overwrite")\
    .options(**target_settings)\
    .save(temp_target_path)

for file in mssparkutils.fs.ls(temp_target_path):
    if file.name.endswith(".parquet"):
        mssparkutils.fs.mv(os.path.join(temp_target_path, file.name), target_path, True, True)
        break

mssparkutils.fs.rm(temp_target_path, True)

print(f"PARQUET saved to {target_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
