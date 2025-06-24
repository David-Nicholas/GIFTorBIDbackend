import json
import boto3
import logging
from decimal import Decimal
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

DYNAMODB_LISTING_TABLE = os.environ['DYNAMODB_LISTING_TABLE']

def lambda_handler(event, context):
    try:

        if 'queryStringParameters' not in event:
            logger.error("Missing 'queryStringParameters'")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'email' in query parameters"})}
        
        params = event.get("queryStringParameters", {})
        logger.info("Query parameters: %s", params)
        user_email = params.get("email")
        logger.info("Received email: %s", user_email)
        path = event.get('resource', '')
        logger.info("Received path: %s", path)
        
        table = dynamodb.Table(DYNAMODB_LISTING_TABLE)

        if path == "/user/listings":
            listings = query_listings_by_email(table, user_email, 'sellerEmail')
            return {
                "statusCode": 200,
                "body": json.dumps(listings, default=str)
            }

        elif path == "/user/redeems":
            redeems = query_listings_by_email(table, user_email, 'redeemerEmail')
            return {
                "statusCode": 200,
                "body": json.dumps(redeems, default=str)
            }

        else:
            return {"statusCode": 404, "body": json.dumps({"error": "Resource not found"})}

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def query_listings_by_email(table, email, email_field):
    """Query listings based on email."""
    from boto3.dynamodb.conditions import Key, Attr

    response = table.scan(
        FilterExpression=Attr(email_field).eq(email)
    )
    return response['Items']
