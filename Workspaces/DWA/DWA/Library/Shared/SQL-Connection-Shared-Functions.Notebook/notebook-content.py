# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# #### SQL-Connection-Shared-Functions
# 
# This notebook is designed to establish a secure connection to a Microsoft Fabric or Power BI-backed SQL endpoint using a short-lived Azure Active Directory (AAD) token

# CELL ********************

import struct
import sqlalchemy
import pyodbc
from notebookutils import mssparkutils
def create_engine(connection_string : str):
    token = mssparkutils.credentials.getToken('https://analysis.windows.net/powerbi/api').encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token)}s', len(token), token)
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    return sqlalchemy.create_engine("mssql+pyodbc://", creator=lambda: pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
