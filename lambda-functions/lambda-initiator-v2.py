import boto3
import botocore
import os
from datetime import datetime

"""
works fine one Lambda function without VPC, but fails when VPC is enabled.
Enabling VPC requires NAT Gateway to allow Lambda to access the internet. And NAT Gateway is not free.
            - also requires a VPC endpoint for S3 to allow Lambda to access S3
            - also requires a VPC endpoint for CloudWatch to allow Lambda to write logs
"""
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

        # Start EMR Cluster
        print("Initializing EMR client...")
        emr_client = boto3.client('emr', region_name='us-east-1')
        print("EMR client initialized.")
        print("Starting EMR cluster...")
        # Define the EMR cluster configuration
        response = emr_client.run_job_flow(
            Name='spark-preprocessing-test-0009',
            LogUri='s3://spark-preprocessing-test-0000/logs',
            ReleaseLabel='emr-7.2.0',
            Instances={
                'InstanceGroups': [
                    {
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                        'Name': 'Primary',
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': 32
                                    },
                                    'VolumesPerInstance': 2
                                }
                            ]
                        }
                    },
                    {
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                        'Name': 'Core',
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': 32
                                    },
                                    'VolumesPerInstance': 2
                                }
                            ]
                        }
                    }
                ],
                'Ec2KeyName': 'emr-sparkle-windows',
                'KeepJobFlowAliveWhenNoSteps': False,  # Ensures cluster terminates after steps
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-0dbd0db9f7e789577',
                'EmrManagedMasterSecurityGroup': 'sg-05630153380ba11bf',
                'EmrManagedSlaveSecurityGroup': 'sg-0e803bcd51b72c05e',
            },
            ServiceRole='arn:aws:iam::541020517509:role/service-role/AmazonEMR-ServiceRole-20240802T115931',
            JobFlowRole='AmazonEMR-InstanceProfile-20240802T115916',
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Hive'},
                {'Name': 'JupyterEnterpriseGateway'},
                {'Name': 'JupyterHub'},
                {'Name': 'Livy'},
                {'Name': 'Spark'}
            ],
            BootstrapActions=[
                {
                    'Name': 'install-boto3',
                    'ScriptBootstrapAction': {
                        'Path': 's3://spark-preprocessing-test-0000/bootstrap-actions/install-boto3.sh'
                    }
                }
            ],
            Steps=[
                {
                    'Name': 'sparkle-v5',
                    'ActionOnFailure': 'CONTINUE',  # TERMINATE_CLUSTER | CANCEL_AND_WAIT | CONTINUE
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'spark-submit',
                            '--deploy-mode', 'cluster',
                            's3://spark-preprocessing-test-0000/spark-apps/sparkle-v5.py',
                            '--input_directory', 's3://spark-preprocessing-test-0000/raw-data/',
                            '--output_directory', 's3://spark-preprocessing-test-0000/processed-data-4/',
                            '--processed_directory', 's3://spark-preprocessing-test-0000/old-raw-data/'
                        ]
                    }
                }
            ],
            ManagedScalingPolicy={
                'ComputeLimits': {
                    'UnitType': 'Instances',
                    'MinimumCapacityUnits': 2,
                    'MaximumCapacityUnits': 10,
                    'MaximumOnDemandCapacityUnits': 6,
                    'MaximumCoreCapacityUnits': 6
                }
            },
            ScaleDownBehavior='TERMINATE_AT_TASK_COMPLETION',
            AutoTerminationPolicy={
                'IdleTimeout': 3600
            },
            Tags=[
                {'Key': 'for-use-with-amazon-emr-managed-policies', 'Value': 'true'}
            ]
        )

        print(f"Started EMR cluster with cluster ID: {response['JobFlowId']}")

    except Exception as e:
        print(f"Error processing event: {e}")
