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

# #### Extract-CSV 
# 
# This notebook automates the loading of CSV files from a Data Lake into staging or bronze tables within a Fabric Lakehouse. It supports archiving, customizable schemas, deduplication, checksums, file archiving, and merge operations. It dynamically processes both single and multiple files, structures the data into organized tables, and maintains full lineage tracking.

# PARAMETERS CELL ********************

SourceSettings = '{"directory": "landing/aw", "file": "Account.csv"}' # "condition" : "target.RowChecksum = source.RowChecksum","mode":"merge"
# Source Setting Options https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html
TargetSettings = '{"schema":"aw_stg", "mode":"overwrite" }'
SourceConnectionSettings = None
TargetConnectionSettings= None
# all of these are optional and set to their default
ActivitySettings = '{"withChecksum" : false, "dedupe": false}'
LineageKey = '00000000-0000-0000-0000-000000000000'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import json
import re
import fnmatch
from pyspark.sql.functions import lit, input_file_name, expr, sha1,concat_ws, col, coalesce
from pyspark.sql import functions as F
from datetime import datetime
import shutil 
from delta.tables import DeltaTable
from pyspark.sql.functions import input_file_name, regexp_extract
import sempy.fabric as fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Settings
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

source_settings = json.loads(SourceSettings or '{}')

source_directory = source_settings["directory"]
del source_settings["directory"]
source_file = source_settings["file"]
del source_settings["file"]

source = f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files"
source_path = os.path.join(source, source_directory)

target_settings = json.loads(TargetSettings or '{}')
target_table = target_settings.get("table")
target_schema = target_settings.get("schema","dbo")
drop = target_settings.pop("drop", True)
write_mode = target_settings.pop("mode", "overwrite")
if write_mode == "merge":
    merge_condition = target_settings.pop("condition")

archive = source # Assumes files are to be archive on the source workspace and lakehouse
activity_settings = json.loads(ActivitySettings or "{}")
dedupe = bool(activity_settings.get("dedupe"))
with_checksum = bool(activity_settings.get("withChecksum"))
archive_directory = activity_settings.get("archivedirectory")
column_names = source_settings.pop("names", None)
source_settings.setdefault("header", True)

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

## Potential function
if not mssparkutils.fs.exists(source_path) or len(mssparkutils.fs.ls(source_path)) == 0:
    mssparkutils.notebook.exit(0) # no folder to process, quit
file_list = mssparkutils.fs.ls(source_path)
if len(file_list) == 0:
    mssparkutils.notebook.exit(0) # no files to process, quit
if source_file:
    files_to_process = [
        os.path.join(source_path, f.name)
        for f in file_list
        if fnmatch.fnmatch(f.name, source_file)]
else:
    files_to_process = [f.path for f in file_list]

if not files_to_process:
    print(f"No files matched pattern: Files/{source_directory}/{source_file}")
    mssparkutils.notebook.exit(0)
#print(files_to_process)

if target_table:
    Files = [files_to_process]
else:
    Files = [[f] for f in files_to_process]
    
if len(Files) > 1 and column_names:
    raise ValueError("Cannot supply column names to CSV's going to multiple tables. (Check the source_settings does not have the 'names' parameter, without a target table.)")

for table_files in Files:
    table_name = target_table or os.path.basename(table_files[0]).split(".")[0]
    # table_name handling for no schema/schema enabled lakehouses
    if not default_schema:
        schema_name = f"dbo"
        table_name = f"{target_schema}_{table_name}" if target_schema else table_name   
    elif len(target_schema)>0:
        schema_name = target_schema
    else:
        schema_name = f"{default_schema}"
    target = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{schema_name}"
    table_path = os.path.join(target, table_name)
    # Write to delta/LH using Spark
    t = datetime.now()

    df = spark.read.options(**source_settings).format("csv").load(table_files)
    in_row_count = df.count()
    print(f"Extracting {', '.join(os.path.basename(f) for f in table_files)} "
    f"from Files/{source_directory} "
    f"into Tables/{schema_name}/{table_name.replace('.', '/')}")

    pattern = r'[ ,\;{}()\n\t=]'
    clean_headers = [re.sub(pattern, '_', col) for col in column_names or df.columns]
    df = df.toDF(*clean_headers)

    if with_checksum:
        df = df.withColumn("RowChecksum", sha1(concat_ws("", *df.columns)))
    if dedupe:
        df = df.drop_duplicates(["RowChecksum"]) if with_checksum else df.drop_duplicates(clean_headers)
    df = df.withColumn("FileName",input_file_name())
    df = df.withColumn("LineageKey", lit(LineageKey))
    out_row_count = df.count()
    if write_mode=="overwrite":
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)
    if dedupe:
        df = df.drop_duplicates(["RowChecksum"]) if with_checksum else df.drop_duplicates()

    if not mssparkutils.fs.exists(table_path):
        write_mode = "overwrite"
    
    if write_mode == "merge":
        target_df = DeltaTab.forPath(spark, table_path)
        target_df.alias("target").merge(df.alias("source"), merge_condition).whenNotMatchedInsertAll().execute()
    else:
        df.write.mode(write_mode).format("delta").save(table_path)

    print(f"- Read  {in_row_count} rows from {len(table_files)} file{'s' if len(table_files) != 1 else ''}"
        f". Wrote {out_row_count} rows in {(datetime.now()-t).total_seconds():.2f} seconds")

    if archive_directory: 
        for file in table_files:
            archive_folder = os.path.join(archive_directory,t.strftime("%Y"),t.strftime("%m"))
            archive_path = os.path.join(archive,archive_folder,os.path.basename(file))
            print(f"Archiving {file} to {archive_folder}.")
            mssparkutils.fs.mv(file, archive_path, True, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
