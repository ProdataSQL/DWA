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

# #### Extract-CSV-Pandas
# 
# This notebook copies a single CSV file using Pandas from a Data lake into a table in the staging schema in Lakehouse. It supports configurable CSV read options (like delimiter and encoding) and writes the data in either append or overwrite mode.

# PARAMETERS CELL ********************

#See https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html for CSV options
SourceSettings = '{"directory" : "unittest/csv/", "file" : "Date_NoHeader.csv", "header":0,"delimiter":",","encoding":"UTF-8"}'
TargetSettings = '{"tableName":"ZipExample", "schema":"dbo", "mode":"overwrite"}'
# all of these are optional and set to their default
SourceConnectionSettings=None
TargetConnectionSettings=None
ActivitySettings=None
LineageKey = 1


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
from pyspark.sql.functions import lit
import pandas as pd
pd.DataFrame.iteritems = pd.DataFrame.items
import sempy.fabric as fabric

# Your existing settings

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

target_lakehouse_details=fabric.FabricRestClient().get(f"/v1/workspaces/{target_workspace_id}/lakehouses/{target_lakehouse_id}")
default_schema=target_lakehouse_details.json().get("properties", {}).get("defaultSchema") # Check if schema is enabled and get default schema

source_settings = json.loads(SourceSettings or "{}")
source_directory = source_settings["directory"]
source_file = source_settings["file"]
del source_settings["directory"]
del source_settings["file"]

source = os.path.join(f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files",source_directory) 

target_settings = json.loads(TargetSettings or "{}")
target_schema = target_settings.get("schema", "dbo") 
target_table = target_settings.get("table", source_file.split(".")[0])
if target_schema != "dbo":
    target_table = f"{target_schema}_{target_table}"
write_mode = target_settings.get("mode", "overwrite")

target = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{target_schema}"

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

# Extract file
 
file_path = os.path.join(source, source_file)
#print(f"source_settings: {source_settings}")
df = pd.read_csv(file_path, **source_settings)    
row_count = df.shape[0]
spark_df = spark.createDataFrame(df)
# Add 'FileName' and 'LineageKey' columns
spark_df = spark_df.withColumn("FileName", lit(source_file))
spark_df = spark_df.withColumn("LineageKey", lit(LineageKey))

table_path = os.path.join(target, target_table)
if write_mode=="overwrite":
        spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
else:
    spark_df.write.mode(mode).format("delta").saveAsTable(table_path)
print(f"Wrote {row_count} rows from '/Files/{file_path.split('/Files/')[-1]}' to /Tables/{target_schema}/{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
