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
SourceSettings = '{"Directory" : "unittest/csv/", "File" : "Date_NoHeader.csv", "header":0,"delimiter":",","encoding":"UTF-8"}'
TargetSettings = '{"TableName":"ZipExample", "SchemaName":"dbo", "mode":"overwrite"}'
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

# Your existing settings
SourceSettings = SourceSettings or "{}"
TargetSettings = TargetSettings or "{}"

source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)

source_directory = source_settings["Directory"]
source_file = source_settings["File"]

del(source_settings["Directory"])
del(source_settings["File"])


target_schema = target_settings.get("SchemaName", "dbo") 
target_table = target_settings.get("TableName", source_file.split(".")[0])

if target_schema != "dbo":
    target_table = f"{target_schema}_{target_table}"


FILES_PREFIX = "Files/"
if not source_directory.startswith(FILES_PREFIX):
   source_directory = os.path.join(FILES_PREFIX, source_directory)

LAKEHOUSE_PREFIX = "/lakehouse/default/"
source_directory = os.path.join(LAKEHOUSE_PREFIX, source_directory)
mode = target_settings.get("mode", "append")

# Extract file  
file_path = os.path.join(source_directory, source_file)

df = pd.read_csv(file_path, **source_settings)    
row_count = df.shape[0]
spark_df = spark.createDataFrame(df)

# Add 'FileName' and 'LineageKey' columns
spark_df = spark_df.withColumn("FileName", lit(source_file))
spark_df = spark_df.withColumn("LineageKey", lit(LineageKey))


if mode == "overwrite":
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
# Write to LH in the specified mode (mode is already set)
spark_df.write.mode(mode).format("delta").saveAsTable(target_table)
print(f"Wrote {row_count} rows from '{file_path[19:]}' to FabricLH.dbo.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
