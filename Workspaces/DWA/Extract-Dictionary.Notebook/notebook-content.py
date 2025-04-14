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

# No Parameters as settings obtained dynamically from default spark lakehouse
edw="DW"
lh="LH"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from builtin.sql_connection_helper import create_engine
import sempy.fabric as fabric
import re
tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
sql_end_point=connection_string= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = "Driver={{ODBC Driver 18 for SQL Server}};Server={}".format(sql_end_point)
pattern = '[ ,;{}()\n\t/=]'
# List Datasets from meta data
engine = create_engine(connection_string)
with engine.connect() as alchemy_connection:  
    sql =f"select  lower(SCHEMA_NAME(schema_id) + '.' + name) as edw_object_name ,  name as edw_table_name, SCHEMA_NAME(schema_id) as schema_name, create_date, modify_date  from {edw}.sys.tables"
    df= pd.read_sql_query (sql, alchemy_connection)
    spark_df=spark.createDataFrame(df).write.mode("overwrite").saveAsTable("dict_edw_tables")
    sql =f"select  lower(SCHEMA_NAME(schema_id) + '.' + name) as edw_object_name ,  name as edw_table_name, SCHEMA_NAME(schema_id) as schema_name, create_date, modify_date  from {lh}.sys.tables"
    df= pd.read_sql_query (sql, alchemy_connection)
    spark_df=spark.createDataFrame(df).write.mode("overwrite").saveAsTable("dict_lh_tables")
#Store Fabric Artefacts
df=fabric.list_items()
df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
spark.createDataFrame(df).write.mode("overwrite").saveAsTable("dict_artefacts")
#List Fabric Workspaces
df_workspaces =fabric.list_workspaces()
df_workspaces=df_workspaces[df_workspaces['Capacity Id'].notna()] 
#List DataSets and Model Tables 
df_datasets =fabric.list_datasets()
tables=[]
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row[1]
    df =fabric.list_tables(workspace=workspace_id, dataset=dataset)
    df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
    df.rename(columns={'name': 'table_name'}, inplace=True)
    df.insert(0, 'dataset', dataset)
    tables.append(df)
df=pd.concat(tables, ignore_index=True)
df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
if not df.empty:
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(f"dict_dataset_tables")
else:
    print("DataFrame is empty. Skipping write to Spark (dict_dataset_tables).") 
#Store Columns for Data Dictionary
columns=[]
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row[1]
    df =fabric.list_tables(workspace=workspace_id, dataset=dataset,include_columns=True)
    df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
    df.rename(columns={'name': 'table_name'}, inplace=True)
    df.insert(0, 'dataset', dataset)
    columns.append(df)
df=pd.concat(columns, ignore_index=True)
if not df.empty:
    df = spark.createDataFrame(df)
    df.write.mode("overwrite").saveAsTable(f"dict_dataset_columns") 
else:
    print("DataFrame is empty. Skipping write to Spark (dict_dataset_columns).")
measures=[]
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row[1]
    df =fabric.list_measures (workspace=workspace_id, dataset=dataset)
    df=df.rename(columns=dict(zip(df.columns, [re.sub(pattern, '_', col.strip(pattern).lower()) for col in df.columns])))
    df.rename(columns={'name': 'table_name'}, inplace=True)
    df.insert(0, 'dataset', dataset)
    measures.append(df)
df=pd.concat(measures, ignore_index=True)
if not df.empty:
    df = spark.createDataFrame(df)
    df.write.mode("overwrite").saveAsTable(f"dict_dataset_measures") 
else:
    print("DataFrame is empty. Skipping write to Spark (dict_dataset_measures).")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
