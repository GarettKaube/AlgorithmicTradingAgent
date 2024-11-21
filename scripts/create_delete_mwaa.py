import boto3
import os

mwaa_client = boto3.client('mwaa', region_name='us-west-1')

def lambda_handler(event, context):
    action = event['action']
    environment_name = os.environ['MWAA_ENV_NAME']
    
    if action == 'create':
        # Create MWAA environment
        response = mwaa_client.create_environment(
            Name=environment_name,
            AirflowVersion='2.10.1', 
            SourceBucketArn='arn:aws:s3:::airflowbucket421654',
            ExecutionRoleArn=os.environ['EXECUTION_ROLE'],
            DagS3Path='dags',
            EnvironmentClass='mw1.small', 
            MaxWorkers=5,
            MinWorkers=1,
            NetworkConfiguration={
                'SecurityGroupIds': ['sg-047a6c465429aef96'],
                'SubnetIds': ["subnet-0b857b511d2f1f9d7","subnet-079102f4919e0f1ef"]
            },
            WebserverAccessMode="PUBLIC_ONLY",
            RequirementsS3Path="requirements.txt"
        )
        return f'MWAA environment {environment_name} created successfully.'

    elif action == 'delete':
        # Delete MWAA environment
        response = mwaa_client.delete_environment(Name=environment_name)
        return f'MWAA environment {environment_name} deleted successfully.'

    return 'No action specified.'
