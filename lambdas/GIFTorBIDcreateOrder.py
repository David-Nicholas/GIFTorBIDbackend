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

        sub = body.get('sub')
        redeemer_email = body.get('redeemerEmail')
        seller_email = body.get('sellerEmail')
        listing_id = body.get("listingID")

        logger.info("Pass extract data %s, %s, %s, %s", sub, redeemer_email, seller_email, listing_id)

        if not sub or not redeemer_email or not seller_email or not listing_id:
            logger.error("Missing required parameters: sub, redeemerEmail, listingID, sellerEmail, listingId")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}
        
        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        redeemer_user_response = user_table.get_item(Key={'userEmail': redeemer_email})

        if 'Item' not in redeemer_user_response:
            logger.error(f"User not found: {redeemer_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        redeemer_user_item = redeemer_user_response['Item']
        if redeemer_user_item['userID'] != sub or listing_id not in redeemer_user_item.get('redeemedIDs', []):
            logger.error("Unauthorized access or listing not found")
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access or listing not found"})}

        logger.info(f"Pass user retrieve for redeemer")

        seller_user_response = user_table.get_item(Key={'userEmail': seller_email})

        if 'Item' not in seller_user_response:
            logger.error(f"User not found: {seller_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        seller_user_item = seller_user_response['Item']
        if listing_id not in seller_user_item.get('listingsIDs', []):
            logger.error("Listing not found in sellers listings")
            return {"statusCode": 403, "body": json.dumps({"error": "Listing not found in sellers listing"})}

        logger.info(f"Pass user retrieve for seller")

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        listing_response = listing_table.get_item(Key={'listingID': listing_id})

        if 'Item' not in listing_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}
    
        listing_item = listing_response['Item']

        logger.info(f"Pass listing retrieve for table")

        if listing_item['status'] == 'orderd' or listing_item['status'] == 'complete':
            return {"statusCode": 404, "body": json.dumps({"error": "Listing was already ordered"})}

        listing_end_date = listing_item['endDate']
        now = datetime.utcnow().isoformat() + "Z"
        logger.info(f"Current time: {now}, {type(now)}")

        listing_end_date_dt = datetime.strptime(listing_end_date, "%Y-%m-%dT%H:%M:%S.%fZ")
        now_dt = datetime.strptime(now, "%Y-%m-%dT%H:%M:%S.%fZ")

        number_part = ''.join([str(random.randint(0, 9)) for _ in range(11)])
        generated_awb = f"GOB{number_part}"

        logger.info("generated awb %s", generated_awb)
        
        order_id = f"order-{listing_id}"
        orders_table = dynamodb.Table(DYNAMODB_ORDERS_TABLE)
        existing_order = orders_table.get_item(Key={'orderID': order_id})

        if 'Item' in existing_order:
            logger.error(f"Order already exists for listingID: {listing_id}")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Order already exists for this listing"})
            }

        pickup_point = f"country: {seller_user_item['country']}, county: {seller_user_item['county']}, city: {seller_user_item['city']}, adress: {seller_user_item['address']} {seller_user_item['postalCode']}"
        drop_point = f"country: {redeemer_user_item['country']}, county: {redeemer_user_item['county']}, city: {redeemer_user_item['city']}, adress: {redeemer_user_item['address']} {redeemer_user_item['postalCode']}"
        order_date = datetime.now().isoformat() + "Z"
        expiration_date = (datetime.now() + timedelta(days=10)).isoformat() + "Z"

        cost = 10
        listing_type = listing_item['type']
        
        if listing_type == 'auction':
            bids = listing_item.get('bids', [])
            cost += bids[0]['amount']

        resource = orders_table.put_item(
            Item={
                'orderID': order_id,
                'awb': generated_awb,
                'listingID': listing_id,
                'sellerEmail': seller_email,
                'sellerPhone': seller_user_item['phoneNumber'],
                'redeemerEmail': redeemer_email,
                'redeemerPhone': redeemer_user_item['phoneNumber'],
                'pickupPoint': pickup_point,
                'dropPoint': drop_point,
                'orderDate': order_date,
                'expirationDate': expiration_date,
                'redeemerReviewed': bool(False),
                'sellerReviewed': bool(False),
                'cost': cost
            }
        )

        update_listing_status = listing_table.update_item(
            Key={'listingID': listing_id},
            UpdateExpression="SET #status = :s",
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={":s": "ordered"},
            ReturnValues="UPDATED_NEW"
        )

        logger.info("Listing updated successfully: %s", update_listing_status)

        notification_message = f"User {redeemer_user_item['name']} ordered item {listing_item['name']}."
        notification = { 
            'message': notification_message, 
            'redirect': '/posts'
        }

        update_sellerUser = user_table.update_item(
                Key={'userEmail': seller_email},
                UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :l)",
                ExpressionAttributeValues={":l": [notification],  ":empty_list": []},
                ReturnValues="UPDATED_NEW"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Order created successfully"
            })
        }

    except Exception as e:
        print("Error creating order:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Failed to create order",
                "error": str(e)
            })
        }
