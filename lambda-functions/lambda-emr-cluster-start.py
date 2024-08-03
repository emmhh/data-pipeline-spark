import boto3

def lambda_handler(event, context):
    # Initialize the boto3 EMR client
    emr_client = boto3.client('emr', region_name='us-east-1')

    # Define the EMR cluster configuration
    try:
        response = emr_client.run_job_flow(
            Name='spark-preprocessing-test-0009',
            LogUri='s3://spark-preprocessing-test-0000/logs',
            ReleaseLabel='emr-7.2.0',
            Instances={
                'InstanceGroups': [
                    {
                        'InstanceCount': 1,
                        'InstanceGroupType': 'MASTER',
                        'Name': 'Primary',
                        'InstanceType': 'm5.xlarge',
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
                        'InstanceCount': 2,
                        'InstanceGroupType': 'CORE',
                        'Name': 'Core',
                        'InstanceType': 'm5.xlarge',
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
            ],
            UnhealthyNodeReplacement=True
        )

        print(f"Started EMR cluster with cluster ID: {response['JobFlowId']}")

    except Exception as e:
        print(f"Error launching EMR cluster: {e}")

