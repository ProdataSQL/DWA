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

# #### Extract-CSV 
# 
# This notebook automates the loading of CSV files from a Data Lake into staging or bronze tables within a Fabric Lakehouse. It supports archiving, customizable schemas, deduplication, checksums, file archiving, and merge operations. It dynamically processes both single and multiple files, structures the data into organized tables, and maintains full lineage tracking.

# PARAMETERS CELL ********************

SourceSettings = '{"Directory": "unittest/AdventureWorks/erp", "File": "*.csv"}' # "condition" : "target.RowChecksum = source.RowChecksum","mode":"merge"
# Source Setting Options https://spark.apache.org/docs/3.5.4/sql-data-sources-csv.html
TargetSettings = '{"SchemaName":"tst", "mode":"overwrite" }'
SourceConnectionSettings = None
SinkConnectionSettings = None
# all of these are optional and set to their default
ActivitySettings = '{"with_checksum" : false, "dedupe": false}'
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

source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
activity_settings = json.loads(ActivitySettings or '{}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_name = spark.conf.get("trident.lakehouse.name")
source_directory = source_settings.pop("Directory")
source_file = source_settings.pop("File", None)
target_table_name = target_settings.pop("TableName", None)
target_schema_name = target_settings.pop("SchemaName", "stg").strip("_. ")
drop = target_settings.pop("drop", True)
write_mode = target_settings.pop("mode", "overwrite")
archive_directory = activity_settings.get("ArchiveDirectory")
do_archive = bool(archive_directory)
dedupe = bool(activity_settings.get("dedupe"))
with_checksum = bool(activity_settings.get("with_checksum"))
column_names = source_settings.pop("names", None)
source_settings.setdefault("header", True)

FILES_PREFIX = "Files"
if not source_directory.startswith(FILES_PREFIX):
    source_directory = os.path.join(FILES_PREFIX, source_directory).replace("\\", "/")

if do_archive and not archive_directory.startswith(FILES_PREFIX):
    archive_directory = os.path.join(FILES_PREFIX, archive_directory).replace("\\", "/")

if write_mode == "merge":
    merge_condition = target_settings.pop("condition")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not mssparkutils.fs.exists(source_directory):
    print("Directory does not exist.")
    mssparkutils.notebook.exit(0)
\ProdataSQL\DWA\blob\main\Scripts\Setup-DWA.ipynb
file_list = mssparkutils.fs.ls(source_directory)
if not file_list:
    print("No files found in the source directory.")
    mssparkutils.notebook.exit(0)

is_wildcard = "*" in source_file or "?" in source_file
files_to_process = [
    os.path.join(source_directory, f.name) for f in file_list if fnmatch.fnmatch(f.name, source_file)
] if source_file else [f.path for f in file_list]

if not files_to_process:
    print("No files matched the specified pattern.")
    mssparkutils.notebook.exit(0)

table_files_mapping = [files_to_process] if target_table_name else [[file] for file in files_to_process]

if len(table_files_mapping) > 1 and column_names:
    raise ValueError("Cannot supply column names to CSV's going to multiple tables. (Check the source_settings does not have the 'names' parameter, without a target table.)")

for table_files in table_files_mapping:
    table_name = target_table_name or os.path.basename(table_files[0]).split(".")[0]

    if target_schema_name:
        table_name = f"{target_schema_name}.{table_name.strip('_ ')}"

    table_name = table_name.strip('_ ')

    print(f"Extracting {', '.join(table_files)} into {lakehouse_name}.dbo.{table_name}.")
    t = datetime.now()

    if write_mode == "overwrite" and drop:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    df = spark.read.options(**source_settings).format("csv").load(table_files)

    pattern = r'[ ,\;{}()\n\t=]'
    clean_headers = [re.sub(pattern, '_', col) for col in column_names or df.columns]
    df = df.toDF(*clean_headers)

    if with_checksum:
        df = df.withColumn("RowChecksum", sha1(concat_ws("", *df.columns)))

    df = df.withColumn("FileName", expr("substring_index(substring_index(input_file_name(), '/', -1), '?', 1)"))

    if dedupe:
        df = df.drop_duplicates(["RowChecksum"]) if with_checksum else df.drop_duplicates(clean_headers)

    df = df.withColumn("LineageKey", lit(LineageKey))

    if not spark.catalog.tableExists(table_name):
        write_mode = "overwrite"

    if write_mode == "merge":
        sink_df = DeltaTable.forPath(spark, f"Tables/{table_name.replace('.', '/')}")
        sink_df.alias("target")\
           .merge(df.alias("source"), merge_condition)\
           .whenNotMatchedInsertAll().execute()
    else:
        df.write.mode(write_mode).format("delta").saveAsTable(table_name)

    if source_file:
        print(f"\t- Wrote {len(table_files)} files into {lakehouse_name}.{table_name} in {(datetime.now()-t).total_seconds()} seconds")
    else:
        print(f"\t- Wrote {'' if source_file else 'all csvs in directory'} \"{table_files}\" into {lakehouse_name}.{table_name} in {(datetime.now()-t).total_seconds()} seconds ")

    if not do_archive: 
        print()
        continue
    
    for f in table_files:
        source_path = os.path.join(source_directory, os.path.basename(f))

        target_path =os.path.join(archive_directory, os.path.basename(f))
        print(f"\t- Archiving {source_path} to {target_path}.")
        mssparkutils.fs.mv(source_path, target_path, True, True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
