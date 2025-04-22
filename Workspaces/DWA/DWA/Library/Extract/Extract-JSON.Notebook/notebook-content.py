# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "19785e4d-5572-4ced-bfab-f26e7c5de3ce",
# META       "default_lakehouse_name": "FabricLH",
# META       "default_lakehouse_workspace_id": "9b8a6500-5ccb-49a9-885b-b5b081efed75",
# META       "known_lakehouses": [
# META         {
# META           "id": "19785e4d-5572-4ced-bfab-f26e7c5de3ce"
# META         },
# META         {
# META           "id": "f2f9c5fa-ca0c-41b2-b0e1-3028165b4f6c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Extract-JSON
# 
# This notebook ingests a JSON file from a configured Data Lake path into a Delta table within the Fabric Lakehouse. It flattens the JSON structure using a specified nodes in ActivitySettings, adds lineage and file metadata, and writes the data to the target table using the defined write mode (e.g., overwrite). 

# PARAMETERS CELL ********************

SourceSettings ='{"Directory" : "unittest/JSON", "File" : "simple.json"}'
TargetSettings ='{"TableName"  : "simpleJSON", "SchemaName" :"dbo","mode":"overwrite" }'
ActivitySettings='{"record_path": "book"}'
SourceConnectionSettings = None
TargetConnectionSettings = None
LineageKey = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import json
import os

SourceSettings=SourceSettings or "{}"
TargetSettings=TargetSettings or "{}"
ActivitySettings=ActivitySettings or '{}'

if ActivitySettings:
    activity_settings = json.loads(ActivitySettings)

source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)

source_directory = source_settings["Directory"]
source_file = source_settings["File"]

target_schema = target_settings.get("SchemaName", "dbo") 
target_table = target_settings.get("TableName", source_file.split(".")[0])

if target_schema != "dbo":
    target_table = f"{target_schema}_{target_table}"


FILES_PREFIX = "Files"
LAKEHOUSE_PREFIX = "/lakehouse/default"
if not source_directory.startswith(FILES_PREFIX):
    source_directory = os.path.join(FILES_PREFIX, source_directory)
if not source_directory.startswith(LAKEHOUSE_PREFIX):
    source_directory = os.path.join(LAKEHOUSE_PREFIX, source_directory)

file_path = os.path.join(source_directory, source_file)


mode = target_settings.get("mode","overwrite")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

with open(file_path, 'r') as f:
    json_data = json.load(f)

df = pd.json_normalize(json_data, **activity_settings)
df["LineageKey"] = LineageKey
df["File"] = source_file

row_count = df.shape[0]

if mode == "overwrite":
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
spark.createDataFrame(df).write.mode(mode).options(**target_settings).format("delta").saveAsTable(target_table)

print(f"Wrote {row_count} rows from {file_path} to FabricLH.dbo.{target_table}.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
