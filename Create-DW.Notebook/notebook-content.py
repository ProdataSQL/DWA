# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# Create Case Insensitve DW Using Semantic Labs
# https://github.com/microsoft/semantic-link-labs
%pip install semantic-link-labs

import sempy_labs
workspace_name="DWA"
warehouse="DW"
print(warehouse)
sempy_labs.create_warehouse (warehouse =warehouse, case_insensitive_collation =True, workspace=workspace_name, description="DWA Enterprise Datawarehouse")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
