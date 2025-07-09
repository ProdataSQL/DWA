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

# PARAMETERS CELL ********************

SourceSettings = '{"Directory" : "landing/AdventureWorks/erp", "File" : "*.xlsx"}'
TargetSettings ='{ "SchemaName": "aw_stg","mode":"overwrite" }'
ActivitySettings = '{"ArchiveDirectory":null,"dedupe":null,"with_checksum":false}'
SourceConnectionSettings=None
TargetConnectionSettings=None
LineageKey = 1

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
from pyspark.sql.functions import lit, input_file_name, expr, xxhash64, col
from pyspark.sql.types import StringType, NullType
from datetime import datetime
import pandas as pd

from delta.tables import DeltaTable
import hashlib
TargetSettings = TargetSettings or '{}'
SourceSettings = SourceSettings or '{}'
ActivitySettings = ActivitySettings or '{}'
activity_settings = json.loads(ActivitySettings)
source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_directory = source_settings["Directory"]
source_file = source_settings["File"]
del source_settings["Directory"]
del source_settings["File"]

FILES_PREFIX = "Files"
if not source_directory.startswith(FILES_PREFIX):
   source_directory = os.path.join(FILES_PREFIX, source_directory).replace("\\", "/")

LAKEHOUSE_DIRECTORY = "/lakehouse/default/"


if "TableName" in target_settings:
   target_table = target_settings.get("TableName")
   del target_settings["TableName"]
else:target_table = target_settings.get("TableName", source_file.split(".")[0])

target_schema = target_settings.get("SchemaName", "stg")

if "sheet_name" in source_settings:
   sheet_name = source_settings["sheet_name"]
   del source_settings["sheet_name"]
else: 
   sheet_name = 0

if "SchemaName" in target_settings:
    del target_settings["SchemaName"]

if target_schema != "dbo":
    target_table_name = f"{target_schema}.{target_table}"

archive_directory = activity_settings.get("ArchiveDirectory")
do_archive = bool(archive_directory)
 
if do_archive and not archive_directory.startswith(FILES_PREFIX):
   archive_directory = os.path.join(FILES_PREFIX, archive_directory).replace("\\", "/")
   
write_mode = target_settings.get("mode", "overwrite") 

if write_mode == "merge":
   merge_condition = target_settings["condition"]
   del target_settings["condition"]

dedupe = bool(activity_settings.get("dedupe"))

with_checksum = bool(activity_settings.get("with_checksum"))

column_names = None
if "names" in source_settings:
   column_names = source_settings["names"]
   del(source_settings["names"])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pathlib import Path

if not mssparkutils.fs.exists(source_directory):
    mssparkutils.notebook.exit(0)

ls = mssparkutils.fs.ls(source_directory)
if len(ls) == 0:
    mssparkutils.notebook.exit(0) # no files to process, quit

Files = [os.path.join(source_directory, file_name) for file_name in fnmatch.filter([file.name for file in ls], source_file)] if source_file is not None else source_directory
error = None

for file in Files:
    file_path = os.path.join(LAKEHOUSE_DIRECTORY, file)
    source_file_base_name   = Path(file_path).stem
    excel_file = pd.ExcelFile(file_path)
    column_names =None

    if "dtype" in source_settings and isinstance(source_settings["dtype"], str):
        source_settings["dtype"] = eval(source_settings["dtype"])

    if sheet_name == "*":
        source_settings["sheet_name"] = None

    elif sheet_name == "":
        source_settings["sheet_name"] = 0
        print("Empty string passed as a sheet name, defaulting to first sheet.")

    if not sheet_name and sheet_name is not None:
        sheets = {excel_file.sheet_names[0] : excel_file.parse(**source_settings)}
    elif isinstance(sheet_name,str) and sheet_name != "*":
        sheets = {sheet_name : excel_file.parse(**source_settings)}
    else:
        sheets = excel_file.parse(**source_settings)


    has_header : bool = True
    if "header" in source_settings and source_settings["header"]=="None":
        source_settings["header"] = None
    
    # First Row as Header normalization
    if "header" in source_settings:
        if isinstance(source_settings["header"], bool):
            has_header = source_settings["header"]
            source_settings["header"] = None if has_header else 1
        elif source_settings["header"] == 0:
            has_header = False
    
    do_drop = write_mode == "overwrite"
    for sheet, df in sheets.items():
        row_count = df.shape[0]
        print (f"- Read  {row_count} rows from '{sheet}' to df")

        #Cleanse Headers
        pattern = '[  \',;{}()\n\t/=%\u00a0]'
        if column_names is None:
            column_names = [re.sub(pattern, '_', col.strip(pattern)) for col in df.columns]
        rename_map = dict(zip(df.columns, column_names))
        df.reset_index(inplace=True)
        if len(sheets) > 1:
            target_table_name =  f"{target_schema}.{source_file_base_name}_{sheet}".lower()
        else:
            if target_table =="*":
                target_table_name = f"{target_schema}.{source_file_base_name}".lower()
            else:
                 target_table_name = target_table_name.lower()
 
         # checksum added BEFORE file and LineageKey columns (obviously)
        if with_checksum:
            df['RowChecksum'] = df.apply(lambda row: hashlib.sha1(''.join(row.astype(str)).encode()).hexdigest(), axis=1)
        df.rename(columns={'index': 'RowNumber'}, inplace=True)
        df = df.rename(columns=rename_map).assign(LineageKey=LineageKey).assign(FileName=os.path.basename(file))

        # Write to delta/LH using Spark
        if do_drop:
            spark.sql(f"DROP TABLE IF EXISTS {target_table_name}")
            do_drop = False

        df = spark.createDataFrame(df)

        if dedupe:
            df = df.drop_duplicates(["RowChecksum"]) if with_checksum else df.drop_duplicates()
        if not target_table_name and not is_wildcard:
            target_table_name = target_table_name or file_info["name"].split(".")[0]

        t = datetime.now()

        if not spark.catalog.tableExists(target_table_name) or write_mode != "merge":
            for column in df.columns:
                if isinstance(df.schema[column].dataType, NullType):
                    df = df.withColumn(column, expr(f"try_cast({column} as STRING)"))
            df.write.mode(write_mode if write_mode != "merge" else "overwrite").format("delta").saveAsTable(target_table_name)

        elif write_mode == "merge":
            target_df = DeltaTab.forPath(spark, f"Tables/{target_table_name}")

            target_df.alias("target")\
                .merge(df.alias("source"), merge_condition)\
                .whenNotMatchedInsertAll().execute()

        end_time = datetime.now()

        print (f"- Write {row_count} rows from df to dbo.{target_table_name} in {(end_time-t).total_seconds()} seconds"  )
    if do_archive:
        archive_source_path = os.path.join(source_directory, os.path.basename(file))
        archive_target_path =os.path.join(archive_directory, os.path.basename(file))
        print(f"Archiving {archive_source_path} to {archive_target_path}.")
        mssparkutils.fs.mv(archive_source_path, archive_target_path, True, True)

        print(f"Archived {file}.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
