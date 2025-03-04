# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from azure.identity import ManagedIdentityCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Define parameters
account_url = "https://onelake.dfs.fabric.microsoft.com"
workspace_name = "DWA"
file_system_name = f"{workspace_name}/Files"

# Get the service client
credential = ManagedIdentityCredential()
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)

# List files in the Lakehouse
file_system_client = service_client.get_file_system_client(file_system_name)
paths = file_system_client.get_paths()

# Print file names
for path in paths:
    print(path.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
