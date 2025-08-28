# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d58f4f2d-59d7-406d-ae4c-898354a6a75f",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "5941a6c0-8c98-4d79-b065-a3789e9e0960",
# META       "known_lakehouses": [
# META         {
# META           "id": "d58f4f2d-59d7-406d-ae4c-898354a6a75f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Export-SQL
# 
# This notebook retrieves data from a SQL source—such as a table, view, or stored procedure—and exports the results to a specified file format (CSV, Excel, Parquet, or Delta). The output is saved to a defined directory within the Fabric Lakehouse. The script supports dynamic query generation based on the source object and allows configurable export options through the target settings, including file type inference, formatting options, and support for Lakehouse-compatible paths.

# PARAMETERS CELL ********************

SourceSettings = '{"sourceObject":"LH.aw_stg.account"}'
TargetSettings = '{"directory": "export/parquet", "file":"account.parquet"}'
SourceConnectionSettings = None
TargetConnectionSettings = None
ActivitySettings = None
LineageKey = 0

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

workspaces = fabric.list_workspaces()

source_connection_settings = json.loads(SourceConnectionSettings or "{}")
source_lakehouse_id = source_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
source_workspace_id = source_connection_settings.get("workspaceId",fabric.get_workspace_id())
source_lakehouse_name = source_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=source_lakehouse_id, workspace=source_workspace_id))
source_workspace_name = workspaces.set_index("Id")["Name"].to_dict().get(source_workspace_id, "Unknown")

target_connection_settings = json.loads(TargetConnectionSettings or '{}')
target_lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
target_workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
target_lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=target_lakehouse_id, workspace=target_workspace_id))
target_workspace_name = workspaces.set_index("Id")["Name"].to_dict().get(target_workspace_id, "Unknown")

source_settings = json.loads(SourceSettings or '{}')
source_object = source_settings.get("sourceObject")

target_settings = json.loads(TargetSettings or '{}')
target_directory = target_settings.pop("directory")
target_file = target_settings.pop("file",source_object.split(".")[-1]) 
target_file_type = target_settings.pop("fileType", "infer").lower()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if source_workspace_name==target_workspace_name:
    print(f"Workspace: {source_workspace_name}")
    if source_lakehouse_name==target_lakehouse_name:
        print(f"Lakehouse: {source_lakehouse_name}")
    else:
        print(f"Source Lakehouse: {source_lakehouse_name}")
        print(f"Target Lakehouse: {target_lakehouse_name}")
else:
    print(f"Source Workspace: {source_workspace_name}, Lakehouse: {source_lakehouse_name}")
    print(f"Target Workspace: {target_workspace_name}, Lakehouse: {target_lakehouse_name}")

EXCEL_FILE_TYPES = ('.xlsx')
CSV_FILE_TYPES = ('.csv', '.txt')
DELTA_FILE_TYPES = ('')
PARQUET_FILE_TYPES = ('.parquet')

if target_file_type == 'infer':
    file_base, file_extension = os.path.splitext(target_file)
    if file_extension in DELTA_FILE_TYPES:
        target_file_type='delta'
    elif file_extension in CSV_FILE_TYPES:
        target_file_type='csv'
    elif file_extension in EXCEL_FILE_TYPES:
        target_file_type='excel'
    elif file_extension in PARQUET_FILE_TYPES:
        target_file_type='parquet'
    else:
        raise ValueError(f"Cannot infer export to file with extension of: {file_extension}")
    print(f"File type inferred to be: {target_file_type}")

is_csv = target_file_type.lower() == 'csv'
is_delta = target_file_type.lower() == 'delta'
is_excel = target_file_type.lower() == 'excel'
is_parquet = target_file_type.lower() == 'parquet'

source_sql_end_point= fabric.FabricRestClient().get(f"/v1/workspaces/{source_workspace_id}/lakehouses/{source_lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
source_connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={source_sql_end_point}"

# List Datasets from meta data
source_engine = create_engine(source_connection_string)

if "usp_" in source_object.lower() and not source_object.lower().startswith("exec"): # sproc without EXEC
    query = f"EXEC {source_object}"
elif " " not in source_object:
    query = f"SELECT * FROM {source_object}"
else:
    query = source_object

if is_csv and "header" not in target_settings:
    target_settings["header"] = True

if is_csv and "escape" not in target_settings:
    target_settings["escape"] = '"'
    
with engine.connect() as alchemy_connection:
    df = pd.read_sql_query(query, alchemy_connection)

target = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Files"
target_path = os.path.join(target, target_directory, target_file)
temp_target_path = os.path.join(target, target_directory, f"_{target_file}")

lh_target_directory = os.path.join(target, target_directory)

if not os.path.exists (lh_target_directory):
    os.makedirs(lh_target_directory)

if target_file_type in ('csv', 'delta'):
    spark_df = spark.createDataFrame(df)

    for column in spark_df.columns:
        if spark_df.schema[column].dataType == NullType() and spark_df.select(col(column)).first()[0] is None:
            spark_df = spark_df.withColumn(column, col(column).cast(StringType()))
    if is_csv: spark_df = spark_df.repartition(1)
    spark_df.write\
        .format("com.databricks.spark.csv" if is_csv else 'delta')\
        .mode("overwrite")\
        .options(**target_settings)\
        .save(temp_target_path if is_csv else target_path)
    
    if target_file_type == 'csv':
        for file in mssparkutils.fs.ls(temp_target_path):
            if file.name.endswith(".csv"):
                mssparkutils.fs.mv(os.path.join(temp_target_path, file.name), target_path, True, True)
                break
        mssparkutils.fs.rm(temp_target_path, True)
elif is_excel:
    df.to_excel(target_path,**target_settings)
elif is_parquet:

    df.to_parquet(target_path,**target_settings)
else:
    raise ValueError(f"Unsupported file type: {target_file_type}" )

print(f"'{query}' exported to '/Files/{target_directory}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
