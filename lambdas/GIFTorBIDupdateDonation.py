import json
import boto3
import uuid
import base64
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

DYNAMODB_USER_TABLE = os.environ['DYNAMODB_USER_TABLE']
DYNAMODB_LISTING_TABLE = os.environ['DYNAMODB_LISTING_TABLE']

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        if 'body' not in event:
            logger.error("Missing 'body' in the event")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'body' in the event"})}
        
        body = json.loads(event['body'])
        sub = body.get('sub')
        redeemer_email = body.get('redeemerEmail')
        seller_email = body.get('sellerEmail')
        listing_id = body.get('listingID')
        listing_name = body.get('name')

        if not sub or not redeemer_email or not listing_id or not listing_name or not seller_email:
            logger.error("Missing required parameters: sub, redeemerEmail, listingID, or name, sellerEmail")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        user_response = user_table.get_item(Key={'userEmail': redeemer_email})

        if 'Item' not in user_response:
            logger.error(f"User not found: {redeemer_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        user_item = user_response['Item']
        if user_item['userID'] != sub or listing_id in user_item.get('listingsIDs', []):
            logger.error("Unauthorized access or listing not found")
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access or listing not found"})}

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        listing_response = listing_table.get_item(Key={'listingID': listing_id})

        if 'Item' not in listing_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}

        if listing_response['Item'].get('sellerEmail') == redeemer_email:
            return {"statusCode": 403, "body": json.dumps({"error": "Cannot redeem your own listing"})}

        if listing_response['Item'].get('status') != 'available':
            return {"statusCode": 403, "body": json.dumps({"error": "Listing already redeemed"})}

        listing_date = datetime.utcnow().isoformat() + "Z"
        update_response = listing_table.update_item(
            Key={'listingID': listing_id},
            UpdateExpression="SET redeemerEmail = :r, #status = :s, endDate = :e",
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={":r": redeemer_email, ":s": "redeemed", ":e": listing_date},
            ReturnValues="UPDATED_NEW"
        )
        
        logger.info("Listing updated successfully: %s", update_response)

        notification_message = f"User {user_item['name']} redeemed the listing '{listing_name}'."
        route = f"/donation/{listing_id}"
        notification = {
            'message': notification_message,
            'redirect': route,
        }
        
        update_sellerUser = user_table.update_item(
            Key={'userEmail': seller_email},
            UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :l)",
            ExpressionAttributeValues={":l": [notification], ":empty_list": []},
            ReturnValues="UPDATED_NEW"
        )

        logger.info("Notifications updated successfully: %s", update_sellerUser)

        update_redeemerUser = user_table.update_item(
            Key={'userEmail': redeemer_email},
            UpdateExpression="SET redeemedIDs = list_append(if_not_exists(redeemedIDs, :empty_list), :l)",
            ExpressionAttributeValues={":l": [listing_id], ":empty_list": []},
            ReturnValues="UPDATED_NEW"
        )

        logger.info("Notifications updated successfully: %s", update_redeemerUser)

        return {"statusCode": 200, "body": json.dumps({"message": "Donation redeemed successfully"})}

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}



