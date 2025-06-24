import boto3
from botocore.exceptions import ClientError
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_USER_TABLE = os.environ['DYNAMODB_USER_TABLE']

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_USER_TABLE)

    user_email = event['request']['userAttributes']['email']
    user_id = event['request']['userAttributes']['sub']
    phone_number = event['request']['userAttributes']['phone_number']
    name = event['request']['userAttributes']['name']

    item = {
        'userEmail': user_email,
        'userID': user_id,
        'phoneNumber': phone_number,
        'name': name,
        'country': '',
        'county': '',
        'city': '',
        'address': '',
        'postalCode': '',
        'averageRating': 0,
        'listingsIDs': [],
        'redeemedIDs': [],
        'wishlistIDs': [],
        'reviews': [],
        'notifications': [],
    }

    try:
        table.put_item(Item=item)
        logger.info(f"User added to DynamoDB: {user_email}")
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        raise Exception(f"Failed to add user to DynamoDB: {e.response['Error']['Message']}")
    
    return event
