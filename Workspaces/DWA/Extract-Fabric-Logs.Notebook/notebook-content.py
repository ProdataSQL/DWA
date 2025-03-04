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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fabric Refresh Logs
import pandas as pd
from delta.tables import *
import sempy.fabric as fabric
import re
from pyspark.sql.types import StructType, StructField,  LongType, StringType,DateType, TimestampType,MapType

tenant_id=spark.conf.get("trident.tenant.id")
workspace_id=spark.conf.get("trident.workspace.id")
lakehouse_id=spark.conf.get("trident.lakehouse.id")
lakehouse_name=spark.conf.get("trident.lakehouse.name")
column_pattern = '[ ,;{}()\n\t/=]' #Pattern to remove invalid columns for lakehouse

# List Datasets/SemanticModels
df_datasets =fabric.list_datasets()

table_name="fabric_refresh_logs"
for row in df_datasets.itertuples(index=True, name='datasets'):
    dataset = row[1]
    df=fabric.list_refresh_requests(dataset=dataset, workspace=workspace_id, top_n=100)
    df=df.rename(columns=dict(zip(df.columns, [re.sub(column_pattern, '_', col.strip(column_pattern).lower()) for col in df.columns])))
    df.insert(0, 'dataset', dataset)
    df.insert(1, 'workspace', workspace_id)
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
