import boto3
import csv
import os

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extract bucket name and key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Check if the file is the ".OK" file
        if not key.endswith('.OK'):
            print("Not an .OK file, exiting.")
            return

        # List all objects in the folder
        folder = os.path.dirname(key) + '/'
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)

        # Extract filenames
        filenames = [obj['Key'] for obj in response.get('Contents', [])]

        # Prepare CSV file
        log_bucket = 'spark-preprocessing-test-0000'
        log_key = 'logs/lambda-log-emr-initiator-spark.csv'

        # Read existing log if it exists
        existing_content = ''
        try:
            obj = s3_client.get_object(Bucket=log_bucket, Key=log_key)
            existing_content = obj['Body'].read().decode('utf-8')
        except s3_client.exceptions.NoSuchKey:
            print("Log file does not exist. It will be created.")

        # Append new data
        new_content = existing_content + ','.join(filenames) + '\n'

        # Upload the log file
        s3_client.put_object(Bucket=log_bucket, Key=log_key, Body=new_content.encode('utf-8'))

        # Delete the ".OK" file
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted .OK file: {key}")

    except Exception as e:
        print(f"Error processing event: {e}")

