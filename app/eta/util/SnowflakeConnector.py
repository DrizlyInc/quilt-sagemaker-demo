from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import json
import boto3
import pandas as pd

def connect_to_snowflake_alchemy(
    warehouse="REPORT", database=None, schema="TEST", role="analyst"
):
    secrets = boto3.client("secretsmanager", region_name="us-east-1")
    snowflake_creds = json.loads(
        secrets.get_secret_value(SecretId="snowflake/analytics_api")["SecretString"]
    )
    if database is None:
        database = snowflake_creds["database"]
    engine = create_engine(
        URL(
            account=snowflake_creds["account"],
            user=snowflake_creds["username"],
            password=snowflake_creds["password"],
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
        )
    )
    connection = engine.connect()
    return connection

def get_dataframe_from_sql(sql):
    connection = connect_to_snowflake_alchemy()
    return pd.read_sql(sql, con =connection)

def get_json_from_sql(sql):
    connection = connect_to_snowflake_alchemy()
    return pd.read_sql(sql, con=connection).to_dict('records')