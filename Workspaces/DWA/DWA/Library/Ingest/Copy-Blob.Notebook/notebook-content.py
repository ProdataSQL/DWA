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
# META       "default_lakehouse_workspace_id": "5941a6c0-8c98-4d79-b065-a3789e9e0960"
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Copy-Blob
# 
# This notebook handles the processing of files from a specified source directory, with support for wildcard file patterns. It allows the files to be moved to a target directory, optionally appending the current date to the target path. The script supports flexible configurations for directory paths, filenames, and file movement, ensuring compatibility with a structured file storage environment such as a Lakehouse.

# PARAMETERS CELL ********************

SourceSettings = "{\"Directory\":\"unittest/AdventureWorks/erp\", \"File\":\"Accoun?.csv\"}"
TargetSettings = "{\"Directory\":\"landing/test/copy_blob\"}"
ActivitySettings = None
SourceConnectionSettings = "{\"DataLakeHouseID\":\"19785e4d-5572-4ced-bfab-f26e7c5de3ce\"}"
TargetConnectionSettings = "{\"DataLakeHouseID\":\"19785e4d-5572-4ced-bfab-f26e7c5de3ce\"}"
LineageKey = "b6125511-ccc5-4bd7-bb99-bfb6ad679169"

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
from pyspark.sql.functions import lit, input_file_name, expr
from datetime import datetime, timezone
import shutil 

TargetSettings = TargetSettings or '{}'
SourceSettings = SourceSettings or '{}'
source_settings = json.loads(SourceSettings)
target_settings = json.loads(TargetSettings)
source_directory = source_settings["Directory"]
source_file = source_settings.get("File", "*")
target_directory = target_settings["Directory"]
target_file = target_settings.get("File","")

is_wildcard = "*" in source_file or "?" in source_file

FILES_PREFIX = "Files"
if not source_directory.startswith(FILES_PREFIX):
   source_directory = os.path.join(FILES_PREFIX, source_directory).replace("\\", "/")

if not target_directory.startswith(FILES_PREFIX):
   target_directory = os.path.join(FILES_PREFIX, target_directory).replace("\\", "/")

move = bool(source_settings.get("Move"))
target_dated = bool(target_settings.get("Dated"))
if target_dated:
    target_directory = os.path.join(target_directory, datetime.now(timezone.utc).strftime('%Y%m%d'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ls = mssparkutils.fs.ls(source_directory)

if len(ls) == 0:
    mssparkutils.notebook.exit(0) # no files to process, quit
    
Files = [f.path for f in ls if fnmatch.fnmatch(f.name, source_file)]

for f in Files:
    target_path = os.path.join(target_directory, target_file if not is_wildcard and target_file else os.path.basename(f))

    if move:
        mssparkutils.fs.mv(f, target_path,True,True) 
        print(f"Moved ", end = "")
    else:
        mssparkutils.fs.cp(f, target_path, True)
        print(f"Copied ", end = "")
    print(f"'{f}' to '{target_path}'.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
