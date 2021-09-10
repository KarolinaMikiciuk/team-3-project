import boto3
import psycopg2
from sqlalchemy import create_engine

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

    connection_type_1 = psycopg2.connect(   #normal
        user=creds['DbUser'],           #temporary credentials from aws-credentials-getter
        password=creds['DbPassword'],
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,   
        port=5439
    )

    link = f"postgresql+psycopg2://{creds['DbUser']}:{creds['DbPassword']}@REDSHIFT_HOST:5439/REDSHIFT_DATABASE"
    #changed sqlalchemy to redshift
    #added .redshift.amazonaws.com

    connection_type_2 = create_engine(link)

    #run creation of tables manually
    return connection_type_2

#reference:
#dialect+driver://username:password@host:port/database

 