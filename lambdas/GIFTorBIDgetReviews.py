import json
import boto3
import logging
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Attr
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

DYNAMODB_USERS_TABLE = os.environ['DYNAMODB_USERS_TABLE']

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        params = event.get("queryStringParameters", {}) or {}
        logger.info("Query parameters: %s", params)

        user_email = params.get("userEmail")

        if not user_email:
            logger.error("Missing 'userEmail' in query parameters")
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing 'userEmail' in query parameters"})
            }

        logger.info("Received userEmail: %s", user_email)

        users_table = dynamodb.Table(DYNAMODB_USERS_TABLE)
        users_response = users_table.get_item(Key={'userEmail': user_email})

        if 'Item' not in users_response:
            logger.error("User not found: %s", user_email)
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "User not found"})
            }
    
        user = users_response['Item']
        logger.info("Fetched user: %s", user)

        response_data = {
            'averageRating': user['averageRating'],
            'reviews': user['reviews'],
            'phoneNumber': user['phoneNumber'],
            'userName': user['name']
        }

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_data, default=str)
        }
    
    except Exception as e:
        logger.error("Error: %s", str(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }