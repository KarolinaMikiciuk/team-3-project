import boto3
from src.service import cleanse_pid
import pandas as pa
from csv import reader

def handle(event, c):
    # Get key and bucket informaition
    key = event['Records'][0]['s3']['object']['key'] #file name
    bucket = event['Records'][0]['s3']['bucket']['name'] #bucket name
    
    # use boto3 library to get object from S3
    s3_client = boto3.client('s3')
    s3_object_response = s3_client.get_object(Bucket = bucket, Key = key)
    #data = s3_object_response['Body'].read().decode('utf-8')
    # i think the line above returns list of lists, where each row is a list
    #no headers in our csv
    cleanse_pid.the_etl_pipe_function(s3_object_response)
    
    
    