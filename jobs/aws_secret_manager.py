import boto3
from botocore.exceptions import ClientError

def get_secret(secret_name, region_name='us-west-2'):

    full_secret_name = 'airflow/variables/' + secret_name

    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=full_secret_name
        )
    except ClientError as e:
        # Handle exceptions
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # The requested secret was not found
            raise e
        else:
            raise e
    else:
        # Secrets Manager decrypts the secret value using the associated KMS key
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = get_secret_value_response['SecretBinary']

    return secret
