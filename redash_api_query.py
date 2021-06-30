#teste novo ambiente
import pyodbc
import json
import requests
import pandas as pd 
#from sqlalchemy import create_engine
#from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy import URL
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


url = "http://redash.rappi.com/api/queries/xxxx/results.json?"

headers = {
  "Authorization": "Key INSERT_KEY",
  'Content-Type': 'application/json'
}
params = ''
payload = dict(max_age=0, parameters=params)

response = requests.post(url, headers=headers, data=payload)
raw = response.text.strip('"')
print('data succesfully queried')
df = pd.json_normalize(json.loads(raw)['query_result']['data']['rows'])

print('dataframe successfully Loaded')


#Connection with Snowflake Server to Update Table

con = snowflake.connector.connect(
  user=USER,
  password=PASSWORD,
  account=ACCOUNT,
  database=DATABASE,
  schema=SCHEMA
)

sql  = """delete from "table"
where 1=1"""
cur = con.cursor()
cur.execute(sql)

print('Previous Data on "table" deleted')

success, nchunks, nrows, _ = write_pandas(con, df,'"table"')

print('"table" successfully updated')
