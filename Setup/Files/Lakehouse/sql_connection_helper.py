import struct
import sqlalchemy
import pyodbc
from notebookutils import mssparkutils
def create_engine(connection_string : str):
    token = mssparkutils.credentials.getToken('https://analysis.windows.net/powerbi/api').encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token)}s', len(token), token)
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    return sqlalchemy.create_engine("mssql+pyodbc://", creator=lambda: pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct}))
