import boto3
from src.service import cleanse_pid
import pandas as pa
import io   
from src.service.db_connect import create_connection


def handle(event, c):
    # Get key and bucket informaition
    key = event['Records'][0]['s3']['object']['key'] #file name
    bucket = event['Records'][0]['s3']['bucket']['name'] #bucket name
    
    # use boto3 library to get object from S3
    s3_client = boto3.client('s3')
    s3_object_response = s3_client.get_object(Bucket = bucket, Key = key)
    #s3_object_response is a dict type that contains all sorts of metadata 
    #related to the request sent
    csv_string = s3_object_response['Body'].read().decode('utf-8')
    #csv_string is <class : str>

    #making a dataframe out of the csv_string
    csv_dataframe = pa.read_csv(io.StringIO(csv_string))
    csv_dataframe.columns = ["datetime","location","customer_name","product","amount_paid","payment_method","card_provider"]
    csv_dataframe = csv_dataframe.drop(columns=["customer_name","card_provider"])
    print(csv_dataframe.head(20))

    connection = create_connection()

    cleanse_pid.the_etl_pipe_function(csv_dataframe,connection)
    
    
    