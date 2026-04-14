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
# META       "known_lakehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Extract-Dictionary
# 
# This notebook collects and stores metadata information from Data Warehouse into Lakehouse for data governance, backup and data cataloguing purpose. Metadata information could be notebook information, datasets, data warehouse tables.
# 
# "SQL-Connection-Shared-Function" is also required to establish connection with data warehouse.

# PARAMETERS CELL ********************

edw="FabricDW"
lh="FabricLH"
meta="Meta"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run SQL-Connection-Shared-Functions 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
#from builtin.sql_connection_helper import create_engine
import sempy.fabric as fabric
import re

tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
sql_end_point=connection_string= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = "Driver={{ODBC Driver 18 for SQL Server}};Server={}".format(sql_end_point)
pattern = '[ ,;{}()\n\t/=]'
items = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/SQLDatabases").json()["value"]
sql_database = next((endpoint for endpoint in items if endpoint['displayName'] == meta))
database_name = sql_database["properties"]["databaseName"]
server_name = sql_database["properties"]["serverFqdn"]
meta_connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={server_name};database={database_name};LongAsMax=YES"

# List Datasets from meta data
engine = create_engine(meta_connection_string)
with engine.connect() as alchemy_connection:
    df_datasets = pd.read_sql_query (f"exec config.usp_OpsDatasets", alchemy_connection)
    if not df_datasets.empty:
        spark_df = spark.createDataFrame(df_datasets)

engine = create_engine(connection_string)
with engine.connect() as alchemy_connection:    
    sql =f"select  lower(SCHEMA_NAME(schema_id) + '.' + name) as edw_object_name ,  name as edw_table_name, SCHEMA_NAME(schema_id) as schema_name, create_date, modify_date  from {edw}.sys.tables"
    df= pd.read_sql_query (sql, alchemy_connection)
    if not df_datasets.empty:
        spark_df=spark.createDataFrame(df_datasets).write.mode("overwrite").saveAsTable("dict_edw_tables")

    sql =f"select  lower(SCHEMA_NAME(schema_id) + '.' + name) as edw_object_name ,  name as edw_table_name, SCHEMA_NAME(schema_id) as schema_name, create_date, modify_date  from {lh}.sys.tables"
    df= pd.read_sql_query (sql, alchemy_connection)
    if not df_datasets.empty:
        spark_df=spark.createDataFrame(df_datasets).write.mode("overwrite").saveAsTable("dict_lh_tables")

#Store Fabric Artefacts
df=fabric.list_items()
df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
spark.createDataFrame(df).write.mode("overwrite").saveAsTable("dict_artefacts")

#List Fabric Workspaces
df_workspaces =fabric.list_workspaces()
df_workspaces=df_workspaces[df_workspaces['Capacity Id'].notna()] 

# Store Model Tables
tables=[]
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row.Dataset
    workspace=row.workspace
    if  not df_workspaces[df_workspaces['Name'] ==workspace].empty:
        df =fabric.list_tables(workspace=workspace, dataset=dataset)
        df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
        df.rename(columns={'name': 'table_name'}, inplace=True)
        df.insert(0, 'dataset', dataset)
        tables.append(df)
if tables:
    df=pd.concat(tables, ignore_index=True)
    df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(f"dict_dataset_tables") 

#Store Columns for Data Dictionary
columns=[]
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row.Dataset
    workspace=row.workspace
    if  not df_workspaces[df_workspaces['Name'] ==workspace].empty:
        df =fabric.list_tables(workspace=workspace, dataset=dataset,include_columns=True)
        df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
        df.rename(columns={'name': 'table_name'}, inplace=True)
        df.insert(0, 'dataset', dataset)
        columns.append(df)
if columns:
    df=pd.concat(columns, ignore_index=True)
    df = spark.createDataFrame(df)
    df.write.mode("overwrite").saveAsTable(f"dict_dataset_columns") 

measures=[]
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row.Dataset
    workspace=row.workspace
    if  not df_workspaces[df_workspaces['Name'] ==workspace].empty:
        df =fabric.list_measures (workspace=workspace, dataset=dataset)
        df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
        df.rename(columns={'name': 'table_name'}, inplace=True)
        df.insert(0, 'dataset', dataset)
        measures.append(df)

if measures:
    df=pd.concat(measures, ignore_index=True)
    df = spark.createDataFrame(df)
    df.write.mode("overwrite").saveAsTable(f"dict_dataset_measures") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
