# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ### Stop and Start Fabric Mirror
# use this to fix and replication/mirror issues on Meta SQLDB<BR>
# Documentation:<BR>
# https://learn.microsoft.com/en-us/fabric/database/sql/start-stop-mirroring-api?tabs=5dot1

# CELL ********************

### STOP MIRROR ###
import sempy.fabric as fabric
import requests

database_name ="Meta"
workspace_id= fabric.get_workspace_id()
items = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/SQLDatabases").json()["value"]
database=next((endpoint for endpoint in items if endpoint["displayName"] == database_name))
database_id=database["id"]

url = f"v1/workspaces/{workspace_id}/sqlDatabases/{database_id}/stopMirroring"
r = fabric.FabricRestClient().post(url)
r.raise_for_status

print (f"Stop Command Sent to {database_name}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

### START MIRROR ###
import sempy.fabric as fabric
import requests

database_name ="Meta"
workspace_id= fabric.get_workspace_id()
items = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/SQLDatabases").json()["value"]
database=next((endpoint for endpoint in items if endpoint["displayName"] == database_name))
database_id=database["id"]

url = f"v1/workspaces/{workspace_id}/sqlDatabases/{database_id}/startMirroring"
r = fabric.FabricRestClient().post(url)
r.raise_for_status

print (f"Start Command Sent to {database_name}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
