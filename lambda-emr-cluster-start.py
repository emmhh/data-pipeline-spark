import boto3
import os

def lambda_handler(event, context):
    # Initialize the boto3 EMR client
    emr_client = boto3.client('emr', region_name='us-east-1')

    # Define the EMR cluster configuration
    cluster_name = 'Transient-EMR-Cluster'
    log_uri = 's3://your-log-bucket/emr-logs/'
    release_label = 'emr-6.10.0'
    master_instance_type = 'm5.xlarge'
    slave_instance_type = 'm5.xlarge'
    instance_count = 3  # 1 master + 2 core nodes

    try:
        response = emr_client.run_job_flow(
            Name=cluster_name,
            LogUri=log_uri,
            ReleaseLabel=release_label,
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': master_instance_type,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Core nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': slave_instance_type,
                        'InstanceCount': instance_count - 1,
                    },
                ],
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'Ec2KeyName': 'your-ec2-key-pair',  # Optional, specify if you need SSH access
                'Ec2SubnetId': 'subnet-0abcdef1234567890',  # Specify your subnet ID
            },
            Applications=[
                {'Name': 'Hadoop'},
                {'Name': 'Spark'},
            ],
            Configurations=[
                {
                    'Classification': 'spark',
                    'Properties': {
                        'maximizeResourceAllocation': 'true'
                    }
                }
            ],
            JobFlowRole='EMR_EC2_DefaultRole',  # The default IAM role for the EC2 instances
            ServiceRole='EMR_DefaultRole',      # The default IAM role for the EMR service
            Tags=[
                {'Key': 'Environment', 'Value': 'Development'},
                {'Key': 'Owner', 'Value': 'YourName'}
            ]
        )

        print(f"Started EMR cluster {cluster_name} with cluster ID: {response['JobFlowId']}")

    except Exception as e:
        print(f"Error launching EMR cluster: {e}")
