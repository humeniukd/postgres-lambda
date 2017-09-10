import sys, psycopg2
from os import environ
import boto3

s3 = boto3.client('s3')

#rds settings
rds_host = environ['RDS_HOSTNAME']
username = environ['RDS_USERNAME']
password = environ['RDS_PASSWORD']
db_name = environ['RDS_DB_NAME']
db_port = environ['RDS_PORT']

try:
    conn = psycopg2.connect(host=rds_host, user=username, password=password, dbname=db_name, port=5432, connect_timeout=5)
except Exception as e:
    print('ERROR: Unexpected error: Could not connect to PgSql instance. %s', str(e))
    sys.exit()

def handler(event, context):
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    response = s3.head_object(Bucket=bucket, Key=key)
    duration = response['Metadata']['duration']
    with conn.cursor() as cur:
        cur.execute('UPDATE mcloud_track SET state = 3, duration = %s WHERE uid = %s', (duration, key[:-4]))
        conn.commit()
    return 'ok'
