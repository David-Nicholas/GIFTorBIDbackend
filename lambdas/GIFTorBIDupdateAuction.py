import json
import boto3
import uuid
import base64
from datetime import datetime, timedelta
import logging
from decimal import Decimal
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

DYNAMODB_USER_TABLE = os.environ['DYNAMODB_USER_TABLE']
DYNAMODB_LISTING_TABLE = os.environ['DYNAMODB_LISTING_TABLE']
S3_BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        if 'body' not in event:
            logger.error("Missing 'body' in the event")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'body' in the event"})}
        
        body = json.loads(event['body'])
        sub = body.get('sub')
        bid_amount = Decimal(str(body.get('bidAmount')))
        bidder_email = body.get('bidderEmail')
        listing_id = body.get('listingID')
        listing_name = body.get('name')

        if not sub or not bidder_email or not listing_id or not listing_name or not bid_amount:
            logger.error("Missing required parameters: sub, bidderEmail, listingID, or name, bidAmount")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        user_response = user_table.get_item(Key={'userEmail': bidder_email})


        if 'Item' not in user_response:
            logger.error(f"User not found: {bidder_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        user_item = user_response['Item']
        if user_item['userID'] != sub or listing_id in user_item.get('listingsIDs', []):
            logger.error("Unauthorized access or listing not found")
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access or listing not found"})}

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        listing_response = listing_table.get_item(Key={'listingID': listing_id})

        if 'Item' not in listing_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}

        if listing_response['Item'].get('sellerEmail') == bidder_email:
            return {"statusCode": 403, "body": json.dumps({"error": "Cannot bid on your own listing"})}

        current_bids = listing_response['Item'].get('bids', [])
        if len(current_bids) > 0:
            logger.info("Current highest bid: %s, %s", current_bids[0]['bidderEmail'], current_bids[0]['amount'])
            if current_bids[0]['bidderEmail'] == bidder_email:
                return {"statusCode": 403, "body": json.dumps({"error": "Cannot outbid your own last bid"})}
            logger.info("Current highest bid: %s", current_bids[0]['amount'])
            if current_bids[0]['amount'] >= bid_amount:
                return {"statusCode": 403, "body": json.dumps({"error": "Bid must be higher than the current highest bid"})}

        bid_date = datetime.utcnow().isoformat() + "Z"
        current_endDate = listing_response['Item'].get('endDate', '')

        logger.info("Not over current end time passed: %s", current_endDate)
        logger.info("Bid date: %s", bid_date)

        current_endDate_dt = datetime.strptime(current_endDate, "%Y-%m-%dT%H:%M:%S.%fZ")
        bid_date_dt = datetime.strptime(bid_date, "%Y-%m-%dT%H:%M:%S.%fZ")

        logger.info("Current end date: %s", current_endDate_dt)
        logger.info("Bid date: %s", bid_date_dt)

        if bid_date_dt > current_endDate_dt:
            return {"statusCode": 403, "body": json.dumps({"error": "Auction has ended"})}

        logger.info("Condition passed")

        if current_endDate_dt - bid_date_dt <= timedelta(minutes=5):
            new_endDate = current_endDate_dt + timedelta(minutes=5)
            listing_table.update_item(
                Key={'listingID': listing_id},
                UpdateExpression="SET endDate = :e",
                ExpressionAttributeValues={":e": new_endDate.isoformat() + "Z"}
            )
            logger.info("Extended auction end time by 5 minutes")

        bidder_name = user_item['name']

        new_bid = {
            'bidderEmail': bidder_email,
            'amount': bid_amount,
            'time': bid_date,
            'bidderName': bidder_name
        }

        current_bids.insert(0, new_bid)

        listing_table.update_item(
            Key={'listingID': listing_id},
            UpdateExpression="SET bids = :b",
            ExpressionAttributeValues={":b": current_bids},
            ReturnValues="UPDATED_NEW"
        )

        if len(current_bids) > 0:
            previous_bidder = current_bids[1]['bidderEmail']
            notification_message = f"Someone outbid you on listing '{listing_response['Item']['name']}'."
            route = f"/auction/{listing_id}"
            notification = { 
                'message': notification_message, 
                'redirect': route
            }
            user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
            user_table.update_item(
                Key={'userEmail': previous_bidder},
                UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :l)",
                ExpressionAttributeValues={":l": [notification], ":empty_list": []}
            )

        return {"statusCode": 200, "body": json.dumps({"message": "Bid successfully placed"})}

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}