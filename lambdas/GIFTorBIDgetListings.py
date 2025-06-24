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

DYNAMODB_LISTING_TABLE = os.environ['DYNAMODB_LISTING_TABLE']

def lambda_handler(event, context):
    try:
        logger.info("Complete event: %s", json.dumps(event))
        path = event.get('resource', '')       
        logger.info("Received path: %s", path)
        
        table = dynamodb.Table(DYNAMODB_LISTING_TABLE)

        if path == "/listings/donations":
            return fetch_listings_by_type(table, 'donation')

        elif path == "/listings/auctions":
            return fetch_listings_by_type(table, 'auction')

        elif path == "/listings":
            return fetch_listings_today(table)

        else:
            return {"statusCode": 404, "body": json.dumps({"error": "Resource not found"})}

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def fetch_listings_by_type(table, listing_type):
    response = table.scan(
        FilterExpression=boto3.dynamodb.conditions.Attr('type').eq(listing_type) & Attr('status').is_in(['available', 'redeemed'])
    )
    logger.info("DynamoDB %s response: %s",listing_type, response)
    listings = response['Items']
    logger.info("Fetched %d %s listings: %s", len(listings), listing_type, listings)
    return {
        "statusCode": 200,
        "body": json.dumps(listings, default=str)
    }

def fetch_listings_today(table):
    today = datetime.now().strftime("%Y-%m-%d")

    response_today = table.scan(
        FilterExpression=Attr('listingDate').begins_with(today)
    )

    response_ending_today = table.scan(
        FilterExpression=Attr('type').eq('auction') & Attr('endDate').begins_with(today) & Attr('status').is_in(['available', 'redeemed'])
    )

    listings_today = response_today.get('Items', [])
    auctions_ending_today = response_ending_today.get('Items', [])

    logger.info("Fetched %d non-auction listings created today and %d auctions ending today", 
                len(listings_today), len(auctions_ending_today))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "listingsToday": listings_today,
            "auctionsEndingToday": auctions_ending_today
        }, default=str)
    }

