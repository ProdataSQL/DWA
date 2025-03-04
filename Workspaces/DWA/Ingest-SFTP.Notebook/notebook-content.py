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

# PARAMETERS CELL ********************

SourceConnectionSettings='{"Host":"prodatasftp.blob.core.windows.net", "Username":"prodatasftp.prodata", "Port":22, "keyvault":"https://kv-fabric-dev.vault.azure.net/", "SecretName":"prodata-sftp-password"}'
SourceSettings='{"Directory": "aw/", "File": "*.*" }'
TargetConnectionSettings=None
TargetSettings='{"Directory": "landing/aw"}'
LineageKey= 0

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

source_settings = json.loads(SourceSettings or '{}')
target_settings = json.loads(TargetSettings or '{}')
source_connection_settings = json.loads(SourceConnectionSettings or '{}')

source_username = source_connection_settings["Username"]
source_keyvault = source_connection_settings["keyvault"]
source_secret = source_connection_settings["SecretName"]
source_hostname = source_connection_settings["Host"]
source_port = int(source_connection_settings.get("Port", 22))
source_directory = source_settings.pop("Directory")
source_file = source_settings.get("File", "*")

target_directory = target_settings["Directory"]
target_file = target_settings.get("File", "")

FILES_PREFIX = "Files"
if not target_directory.startswith(FILES_PREFIX):
    target_directory = os.path.join(FILES_PREFIX, target_directory)

source_delete = bool(source_settings.get("deleteFilesAfterCompletion", False))
if "File" in source_settings:
    del source_settings["File"]

contains_wildcard = "*" in source_file or "?" in source_file
if not contains_wildcard:
    target_file = target_settings.get("File", "")

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
    mssparkutils.fs.mkdirs(target_directory)
    target_path = os.path.join("/lakehouse/default/", target_directory, file_name)
    
    with open(target_path, 'wb') as f:
        with sftp.open(remote_path, 'rb') as sftp_file:
            file_data = sftp_file.read()
            f.write(file_data)

    print(f"Written '{remote_path}' to '{target_path}'", end="")
    

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
