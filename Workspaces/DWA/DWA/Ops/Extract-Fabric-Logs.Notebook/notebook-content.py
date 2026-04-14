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

# #### Extract-Fabric-Logs 
# 
# This notebook collects and stores refresh log data from datasets into delta lake table that can be used for monitoring and auditing purpose. 
# It retrieves latest refresh request logs for each dataset in a workspace.

# PARAMETERS CELL ********************

# No Parameters as settings obtained dynamically from default spark lakehouse


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric Refresh Logs
import pandas as pd
from builtin.sql_connection_helper import create_engine
from delta.tables import *
import sempy.fabric as fabric
import re
from pyspark.sql.types import StructType, StructField,  LongType, StringType,DateType, TimestampType,MapType

tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
sql_end_point= fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}").json()['properties']['sqlEndpointProperties']['connectionString']
connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_end_point}"
column_pattern = '[ ,;{}()\n\t/=]' #Pattern to remove invalid columns for lakehouse

engine = create_engine(connection_string)
with engine.connect() as alchemy_connection:
    df_datasets = pd.read_sql_query (f"exec Meta.config.usp_OpsDatasets", alchemy_connection)

table_name="fabric_refresh_logs"
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row.Dataset
    workspace = row.workspace
    df=fabric.list_refresh_requests(dataset=dataset, workspace=workspace, top_n=100)
    df=df.rename(columns=dict(zip(df.columns, [re.sub(column_pattern, '_', col.strip(column_pattern).lower()) for col in df.columns])))
    df.insert(0, 'dataset', dataset)
    df.insert(1, 'workspace', workspace)
    df['refresh_attempts'] = df['refresh_attempts'].astype(str) 
    df.drop(columns=['extended_status'])

    schema = StructType([
    StructField("dataset", StringType(), True),
    StructField("workspace", StringType(), True),
    StructField("id",  LongType(), True),
    StructField("request_id", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("refresh_type", StringType(), True),
    StructField("service_exception_json", StringType(), True),
    StructField("status", StringType(), True),
    StructField("refresh_attempts", StringType(), True)
    ])

    spark_df =spark.createDataFrame(df,schema =schema )
    if spark.catalog.tableExists(table_name):
        target_table = DeltaTable.forName(spark, f"{table_name}")
        target_table.alias("target").merge(spark_df.alias("source"), "source.id=target.id").whenNotMatchedInsertAll().execute()
    else:
        spark_df.write.mode('overwrite').saveAsTable(table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
