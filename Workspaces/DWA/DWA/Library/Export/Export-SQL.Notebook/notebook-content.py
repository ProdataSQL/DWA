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

# #### Export-SQL
# 
# This notebook retrieves data from a SQL source—such as a table, view, or stored procedure—and exports the results to a specified file format (CSV, Excel, Parquet, or Delta). The output is saved to a defined directory within the Fabric Lakehouse. The script supports dynamic query generation based on the source object and allows configurable export options through the target settings, including file type inference, formatting options, and support for Lakehouse-compatible paths.

# PARAMETERS CELL ********************

SourceSettings ='{"SourceObject":"FabricDW.config.Configurations"}'
TargetSettings ='{"Directory": "export/parquet", "File":"Configurations.parquet"}'
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

EXCEL_FILE_TYPES = ('.xlsx')
CSV_FILE_TYPES = ('.csv', '.txt')
DELTA_FILE_TYPES = ('')
PARQUET_FILE_TYPES = ('.parquet')


SourceSettings = SourceSettings or '{}'
TargetSettings = TargetSettings or '{}'

source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)



target_directory = target_settings["Directory"]
target_file = target_settings["File"]
target_file_type = target_settings.pop("FileType", "infer").lower()

FILES_PREFIX = "Files"
LAKEHOUSE_DEFAULT_PREFIX = "/lakehouse/default/"
if not target_directory.startswith(FILES_PREFIX):
   target_directory = os.path.join(FILES_PREFIX, target_directory).replace("\\", "/")

target_path = os.path.join(target_directory, target_file)

temp_target_path = os.path.join(target_directory, f"_{target_file}")

del target_settings["Directory"]
del target_settings["File"]

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



tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
sql_end_point= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point}"
pattern = '[ ,;{}()\n\t/=]'


# List Datasets from meta data
engine = create_engine(connection_string)



source_object = source_settings.get("SourceObject")

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

lh_target_directory = os.path.join(LAKEHOUSE_DEFAULT_PREFIX, target_directory)
lh_target_path = os.path.join(lh_target_directory, target_file)

if not os.path.exists (target_directory) and is_excel or is_delta:
    os.makedirs(target_directory)


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
    df.to_excel(lh_target_path,**target_settings)
elif is_parquet:

    df.to_parquet(lh_target_path,**target_settings)
else:
    raise ValueError(f"Unsupported file type: {target_file_type}" )

print(f"'{query}' exported to '{target_path}'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
