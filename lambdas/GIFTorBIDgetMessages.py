import json
import boto3
import logging
from decimal import Decimal
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

DYNAMODB_USER_TABLE = os.environ['DYNAMODB_USER_TABLE']

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        params = event.get("queryStringParameters", {}) or {}
        logger.info("Query parameters: %s", params)

        user_id = params.get("userID")

        if not user_id:
            logger.error("Missing 'userID' in query parameters")
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing 'userID' in query parameters"})
            }

        logger.info("Received userID: %s", user_id)

        table = dynamodb.Table(DYNAMODB_USER_TABLE)
        response = table.query(
            IndexName='userID-index',
            KeyConditionExpression='userID = :uid',
            ExpressionAttributeValues={':uid': user_id},
            ProjectionExpression="notifications"
        )

        if not response.get('Items'):
            logger.error("User not found: %s", user_id)
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "User not found"})
            }

        user = response['Items'][0]
        logger.info("Fetched user: %s", user)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(user, default=str)
        }

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }
