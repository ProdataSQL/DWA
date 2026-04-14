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

# ## Extract-Artefacts
# Collects and stores metadata information from Data Warehouse into Lakehouse for data governance, backup and data cataloguing purpose.<BR>Metadata information could be notebook information, datasets, data warehouse tables.<BR><BR>
# This is <b>required</b> to populate dict_artefacts for data dictionary and DWA Framework
# 


# CELL ********************

%run SQL-Connection-Shared-Functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
import re
import pandas as pd

workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")

items = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/SQLDatabases").json()["value"]
sql_database=next((endpoint for endpoint in items if endpoint["displayName"] == "Meta"))
sql_end_point = sql_database["properties"]["serverFqdn"]
sql_database_name = sql_database["properties"]["databaseName"]
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point};database={sql_database_name}"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

engine = create_engine(connection_string)
connection = engine.raw_connection()

df=pd.DataFrame()
with engine.connect() as alchemy_connection:
    workspaces = pd.read_sql_query ("exec [config].[usp_GetWorkspaces]", alchemy_connection)
    for ws_id in workspaces['WorkspaceID']:
        print (ws_id)
        items = fabric.list_items(workspace=ws_id)
        if df.empty:
            df=items
        else:
            df=pd.concat([df,items], ignore_index=True)
pattern = '[ ,;{}()\n\t/=]'
df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write Back to Meta SqlDatabase (used for DWA Engine)
from sqlalchemy import text
with engine.begin() as alchemy_connection:
    df.to_sql('dict_artefacts', con=engine, if_exists='replace', index=False,method='multi', chunksize=1000)
    alchemy_connection.execute(text("EXEC [config].[usp_PipelineBuildMetaData]"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
