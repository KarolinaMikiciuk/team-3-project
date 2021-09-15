import boto3
import sqlalchemy
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def create_connection():
    client = boto3.client('redshift', region_name='eu-west-1')
    #returns password + username [temporary]

    REDSHIFT_USER = "awsuser"
    REDSHIFT_CLUSTER = "redshiftcluster-fbtitpjkbelw"
    REDSHIFT_HOST = "redshiftcluster-fbtitpjkbelw.cnvqpqjunvdy.eu-west-1.redshift.amazonaws.com"
    REDSHIFT_DATABASE = "team3db"


    creds = client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DATABASE,
        ClusterIdentifier=REDSHIFT_CLUSTER,
        DurationSeconds=3600)
    
    connect_url = sqlalchemy.engine.url.URL(
    'postgresql+psycopg2',
    username=creds['DbUser'],
    password= quote_plus(creds['DbPassword']),
    host= REDSHIFT_HOST,
    port= 5439
    )

    engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (
    quote_plus(creds['DbUser']),
    quote_plus(creds['DbPassword']),
    REDSHIFT_HOST,
    5439,
    REDSHIFT_DATABASE
    )
    
    engine = create_engine(engine_string)
    connection =engine.connect()

    #run creation of tables manually
    return connection


 