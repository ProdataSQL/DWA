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

# #### Ingest-SFTP
# 
# This notebook connects to a remote server using SSH and SFTP, downloads files from a specified directory (with optional wildcard matching), and saves them to a target location in a Fabric Lakehouse environment. It supports optional file deletion from the source after successful transfer. It handles dynamic file naming and directory creation, ensuring compatibility with Lakehouse storage.


# PARAMETERS CELL ********************

SourceConnectionSettings = '{"host":"prodatasftp.blob.core.windows.net", "userName":"prodatasftp.prodata", "port":22, "keyVault":"https://kv-fabric-dev.vault.azure.net/", "secret":"prodata-sftp-password"}'
SourceSettings = '{"directory": "aw/", "file": "*.*" }'
TargetConnectionSettings = None
TargetSettings = '{"directory": "Files/landing/aw"}'
LineageKey = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import paramiko
import os
import glob
import json
import regex as re
import sempy.fabric as fabric
import pandas as pd
import shutil

source_connection_settings = json.loads(SourceConnectionSettings or '{}')
source_username = source_connection_settings["userName"]
source_keyvault = source_connection_settings["keyVault"]
source_secret = source_connection_settings["secret"]
source_hostname = source_connection_settings["host"]
source_port = int(source_connection_settings.get("port", 22))

source_settings = json.loads(SourceSettings or '{}')
source_directory = source_settings["directory"]
source_file = source_settings.get("file", "*")
source_delete = bool(source_settings.get("delete", False))
if "file" in source_settings:
    del source_settings["file"]


target_connection_settings = json.loads(TargetConnectionSettings or '{}')
lakehouse_id = target_connection_settings.get("lakehouseId",fabric.get_lakehouse_id())
workspace_id = target_connection_settings.get("workspaceId",fabric.get_workspace_id())
lakehouse_name = target_connection_settings.get("lakehouse",fabric.resolve_item_name(item_id=lakehouse_id, workspace=workspace_id))
workspace_name = fabric.list_workspaces().set_index("Id")["Name"].to_dict().get(workspace_id, "Unknown")


target_settings = json.loads(TargetSettings or '{}')
target_directory = target_settings["directory"]
if target_directory.startswith("Files/"):
    target_directory = target_directory[len("Files/"):]
target_file = target_settings.get("file", "")
#Todo: Write files to abss path when required
#target = os.path.join(f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files",target_directory)

contains_wildcard = "*" in source_file or "?" in source_file
if not contains_wildcard:
    target_file = target_settings.get("file", "")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Workspace: {workspace_name}")
print(f"Lakehouse: {lakehouse_name}")
    
transport = paramiko.Transport((source_hostname, source_port))
password = mssparkutils.credentials.getSecret(source_keyvault, source_secret)
transport.connect(username=source_username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)

try:
    files = sftp.listdir(source_directory)
except IOError as e:
    print(e)
    mssparkutils.notebook.exit(f"Directory {source_directory} does not exist.")

files = [file for file in files if glob.fnmatch.fnmatch(file, source_file)]

for index, file_name in enumerate(files):
    remote_path = os.path.join(source_directory, file_name).replace("\\", "/")
    if not contains_wildcard and target_file:
        file_name = target_file
    mssparkutils.fs.mkdirs(target)
    target_path = os.path.join(f'/lakehouse/default/Files', target_directory, file_name)
    
    with open(target_path, 'wb') as f:
        with sftp.open(remote_path, 'rb') as sftp_file:
            file_data = sftp_file.read()
            f.write(file_data)
    print(f"Written '{remote_path}' to '{lakehouse_name}/Files/{target_directory}/{file_name}'", end="")

    if source_delete:
        sftp.remove(remote_path)
        print(" and has been deleted", end="")
    
    print(f". ({index + 1}/{len(files)})")

sftp.close()
transport.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
