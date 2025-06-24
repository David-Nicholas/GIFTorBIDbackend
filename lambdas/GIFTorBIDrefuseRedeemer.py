import json
import boto3
import uuid
import base64
from datetime import datetime, timedelta
import logging
import random
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

DYNAMODB_USER_TABLE = os.environ['DYNAMODB_USER_TABLE']
DYNAMODB_ORDERS_TABLE = os.environ['DYNAMODB_ORDERS_TABLE']
DYNAMODB_LISTING_TABLE = os.environ['DYNAMODB_LISTING_TABLE']

def lambda_handler(event, context):
    try:
        if 'body' not in event:
            logger.error("Missing 'body' in the event")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'body' in the event"})}

        body = json.loads(event['body'])
        logger.info(f"Received body: {body}")

        sub = body.get('sub')
        redeemer_email = body.get('redeemerEmail')
        seller_email = body.get('sellerEmail')
        listing_id = body.get("listingID")

        if not sub or not redeemer_email or not seller_email or not listing_id:
            logger.error("Missing required parameters")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        redeemer_user_response = user_table.get_item(Key={'userEmail': redeemer_email})
        if 'Item' not in redeemer_user_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Redeemer not found"})}
        redeemer_user_item = redeemer_user_response['Item']

        seller_user_response = user_table.get_item(Key={'userEmail': seller_email})
        if 'Item' not in seller_user_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Seller not found"})}
        seller_user_item = seller_user_response['Item']

        if seller_user_item['userID'] != sub:
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized"})}

        if listing_id not in seller_user_item.get('listingsIDs', []):
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found or unauthorized"})}

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        listing_response = listing_table.get_item(Key={'listingID': listing_id})
        if 'Item' not in listing_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}
        listing_item = listing_response['Item']

        if listing_item['status'] in ['ordered', 'redeemed']:
            if listing_item['status'] == 'ordered':
                orders_table = dynamodb.Table(DYNAMODB_ORDERS_TABLE)
                order_id = f"order-{listing_id}"
                orders_table.delete_item(Key={'orderID': order_id})

            if listing_id in redeemer_user_item.get('redeemedIDs', []):
                redeemer_user_item['redeemedIDs'].remove(listing_id)
                user_table.update_item(
                    Key={'userEmail': redeemer_email},
                    UpdateExpression="SET redeemedIDs = :r",
                    ExpressionAttributeValues={":r": redeemer_user_item['redeemedIDs']}
                )

            now = datetime.utcnow().isoformat() + "Z"

            update_expression = "SET #status = :status, redeemerEmail = :empty, winnerEmail = :empty, listingDate = :now"
            expression_values = {":status": "available", ":empty": "", ":now": now}
            expression_names = {"#status": "status"}

            if listing_item['type'] == 'donation':
                update_expression += ", endDate = :empty"
            else:
                update_expression += ", endDate = :new_end, bids = :empty_list"
                duration = int(listing_item['duration'])
                new_end = (datetime.utcnow() + timedelta(days=duration)).isoformat() + "Z"
                expression_values.update({":empty_list": [], ":new_end": new_end})

            listing_table.update_item(
                Key={'listingID': listing_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames=expression_names
            )

            notification = {
                'message': f"The seller refused your redemption for listing '{listing_item['name']}' due to low rating.",
                'redirect': '/auctions' if listing_item['type'] == 'auction' else '/donations'
            }
            user_table.update_item(
                Key={'userEmail': redeemer_email},
                UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :n)",
                ExpressionAttributeValues={":n": [notification], ":empty_list": []}
            )

            return {"statusCode": 200, "body": json.dumps({"message": "Redemption cancelled and listing reset"})}

        else:
            return {"statusCode": 400, "body": json.dumps({"error": "Listing is not in an ordered or redeemed state"})}

    except Exception as e:
        logger.error(f"Error processing refusal: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"message": "Internal server error", "error": str(e)})}
