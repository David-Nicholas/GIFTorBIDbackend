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
        logger.info("Received event: %s", json.dumps(event))

        params = event.get("queryStringParameters", {}) or {}
        logger.info("Query parameters: %s", params)

        listing_id = params.get("listingID")

        if not listing_id:
            logger.error("Missing 'listingID' in query parameters")
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Missing 'listingID' in query parameters"})
            }

        logger.info("Received listingID: %s", listing_id)

        table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        response = table.get_item(Key={'listingID': listing_id})

        if 'Item' not in response:
            logger.error("Listing not found: %s", listing_id)
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Listing not found"})
            }

        listing = response['Item']
        logger.info("Fetched listing: %s", listing)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(listing, default=str)
        }

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }
