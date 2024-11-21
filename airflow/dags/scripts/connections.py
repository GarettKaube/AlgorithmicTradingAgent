import snowflake.connector
import boto3
import ast


def get_secret(secret_name):
    """ Retrieves secrets from AWS Secret Manager
    Parameters
    ----------
    - secret_name: str
      name of secret to retrieve
    
    Returns
    -------
    - list of secret values
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_name)

    secret = response['SecretString']
    return list(ast.literal_eval(secret).values())



def connect_to_snowflake(username, password, account_id, schema):
    """ Connects to snowflake with schema
    Parameters
    ----------
    - schema: str
      name of schema to use
    
    Returns
    -------
    - cur
      snowflake cursor
    - conn
      snowflake connection
    """
    conn = snowflake.connector.connect(
        user=str(username),
        password=str(password),
        account=account_id,
        warehouse='COMPUTE_WH',
        database='DATA_ENG_DBT',
        schema=schema,
        role="DBT_EXECUTOR_ROLE"
    )

    cur = conn.cursor()

    return cur, conn