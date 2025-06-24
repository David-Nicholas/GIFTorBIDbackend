import json
import boto3
import os


dynamodb = boto3.resource('dynamodb')

DYNAMODB_CONNECTION_TABLE = os.environ['DYNAMODB_CONNECTION_TABLE']

def lambda_handler(event, context):
    connectionId = event['requestContext']['connectionId']

    connections_table = dynamodb.Table(DYNAMODB_CONNECTION_TABLE)
    connections_table.delete_item(
        Key={
            'connectionID': connectionId
        }
    )

    return {}