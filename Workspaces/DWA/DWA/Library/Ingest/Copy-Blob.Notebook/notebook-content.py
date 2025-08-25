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

SourceSettings = "{\"directory\":\"unittest/AdventureWorks/erp\", \"file\":\"Accoun?.csv\"}"
TargetSettings = "{\"directory\":\"landing/test/copy_blob\"}"
ActivitySettings = None
SourceConnectionSettings = None
TargetConnectionSettings = None
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
import sempy.fabric as fabric

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
 
source_settings = json.loads(SourceSettings or '{}')
source_directory = source_settings["directory"]
source_file = source_settings.get("file", "*")
is_wildcard = "*" in source_file or "?" in source_file
move = bool(source_settings.get("move"))

target_settings = json.loads(TargetSettings or '{}')
target_directory = target_settings["directory"]
target_file = target_settings.get("file","")
target_dated = bool(target_settings.get("dated"))
if target_dated:
    target_directory = os.path.join(target_directory, datetime.now(timezone.utc).strftime('%Y%m%d'))

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

ls = mssparkutils.fs.ls(f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_lakehouse_id}/Files/{source_directory}")

if len(ls) == 0:
    mssparkutils.notebook.exit(0) # no files to process, quit
    
Files = [f.path for f in ls if fnmatch.fnmatch(f.name, source_file)]

for f in Files:
    target_path = os.path.join(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Files/",target_directory)
    target_file = target_file if not is_wildcard and target_file else os.path.basename(f)
    target = os.path.join(target_path,target_file)
    if move:
        mssparkutils.fs.mv(f, target,True,True) 
        print(f"Moved ", end = "")
    else:
        mssparkutils.fs.cp(f, target, True)
        print(f"Copied ", end = "")
    print(f"'{f.split('/Files/')[1]}' to '{target_directory}/{target_file}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
