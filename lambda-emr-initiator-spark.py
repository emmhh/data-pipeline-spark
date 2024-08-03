import boto3
import botocore
import os
from datetime import datetime

# Configure the S3 client to use path-style addressing
s3_client = boto3.client(
    's3',
    region_name='us-east-1',
    config=botocore.config.Config(s3={'addressing_style': 'path'})
)

def lambda_handler(event, context):
    print("Lambda function started.")

    try:
        # Debugging: Log the received event
        print(f"Received event: {event}")

        # Extract bucket name and key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing bucket: {bucket}, key: {key}")

        # Check if the file is exactly named ".OK"
        if os.path.basename(key) != ".OK":
            print("File is not named .OK, exiting.")
            return

        # Use the folder path directly from the key
        folder = key.rsplit('/', 1)[0] + '/'
        print(f"Listing objects in bucket {bucket}, folder: {folder}")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        
        print(f"Debug## processing {folder}, response is {response}")
        
        # Extract filenames
        filenames = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'] != key]

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

        # Append new data with current datetime
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        new_content = existing_content + f"{current_time}," + ','.join(filenames) + '\n'

        # Upload the log file
        s3_client.put_object(Bucket=log_bucket, Key=log_key, Body=new_content.encode('utf-8'))
        print(f"Log updated in bucket {log_bucket}, key: {log_key}")

        # Delete the ".OK" file
        s3_client.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted .OK file: {key}")

    except Exception as e:
        print(f"Error processing event: {e}")
