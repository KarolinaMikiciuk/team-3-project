import boto3
import psycopg2

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

    connection = psycopg2.connect(   #normal
        user=creds['DbUser'],           #temporary credentials from aws-credentials-getter
        password=creds['DbPassword'],
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,   
        port=5439
    )

    #run creation of tables manually
    return connection

