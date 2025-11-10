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
# META       "default_lakehouse_workspace_id": "5941a6c0-8c98-4d79-b065-a3789e9e0960",
# META       "known_lakehouses": [
# META         {
# META           "id": "d58f4f2d-59d7-406d-ae4c-898354a6a75f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip instal openai


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.synapsesql("DW.aw.DimDate").distinct().limit(10)

display (df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants  
df = spark.read.synapsesql("DW.aw.DimProduct").select("ProductModel","ProductDescription").distinct().limit(10)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
