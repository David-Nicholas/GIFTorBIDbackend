import json
import boto3
import uuid
import base64
from datetime import datetime, timedelta, timezone
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
        writer_email = body.get('writerEmail')
        listing_id = body.get('listingID')
        message = body.get('message')
        rating = body.get('rating')

        if not sub or not writer_email or not listing_id or not message or not rating:
            logger.error("Missing required parameters: sub, userEmail, listingID, message, rating")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters, listingID, message, rating"})}

        users_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        orders_table = dynamodb.Table(DYNAMODB_ORDERS_TABLE)
        listings_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)

        writer_response = users_table.get_item(Key={'userEmail': writer_email})
        if 'Item' not in writer_response:
            logger.error(f"User not found: {writer_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}
        writer_item = writer_response['Item']

        if writer_item['userID'] != sub:
            logger.error("User info not matching anyone in user pool")
            return {"statusCode": 403, "body": json.dumps({"error": "User info not matching anyone in user pool"})}
        
        order_id = f"order-{listing_id}"
        order_response = orders_table.get_item(Key={'orderID': order_id})
        order_exists = 'Item' in order_response

        listing_response = listings_table.get_item(Key={'listingID': listing_id})
        if 'Item' not in listing_response:
            logger.error(f"Listing not found: {listing_id}")
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}
        listing_item = listing_response['Item']

        if order_exists:
            order_item = order_response['Item']
            is_seller = order_item['sellerEmail'] == writer_email
            is_redeemer = order_item['redeemerEmail'] == writer_email

            if not (is_seller or is_redeemer):
                logger.error("Unauthorized access")
                return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access"})}
            
            now = datetime.utcnow().isoformat() + "Z"
            logger.info(f"Current time: {now}, {type(now)}")

            expiration_date = order_item['expirationDate']
            logger.info(f"Expiration date: {expiration_date}, {type(expiration_date)}")
        
            now_dt = datetime.strptime(now, "%Y-%m-%dT%H:%M:%S.%fZ")
            expiration_date_dt = datetime.strptime(expiration_date, "%Y-%m-%dT%H:%M:%S.%fZ")

            if now < expiration_date:
                logger.error("Order not expired yet")
                return {"statusCode": 400, "body": json.dumps({"error": "Order not expired yet"})}

            if is_seller:
                if order_item['redeemerReviewed'] == True:
                    logger.error("Redeemer already reviewed")
                    return {"statusCode": 400, "body": json.dumps({"error": "Redeemer already reviewed"})}

            if is_redeemer:
                if order_item['sellerReviewed'] == True:
                    logger.error("Seller already reviewed")
                    return {"statusCode": 400, "body": json.dumps({"error": "Seller already reviewed"})}

            reviewed_email = order_item['sellerEmail'] if is_redeemer else order_item['redeemerEmail']

        else:
            if listing_item['sellerEmail'] != writer_email:
                logger.error("Unauthorized, only seller can review without order")
                return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized"})}
            
            listing_end_date = listing_item['endDate']
            now = datetime.utcnow().isoformat() + "Z"
            logger.info(f"Current time: {now}, {type(now)}")

            listing_end_date_dt = datetime.strptime(listing_end_date, "%Y-%m-%dT%H:%M:%S.%fZ")
            now_dt = datetime.strptime(now, "%Y-%m-%dT%H:%M:%S.%fZ")

            if  now_dt - listing_end_date_dt <= timedelta(days=2):
                return {"statusCode": 404, "body": json.dumps({"error": "The redeemer still has time to order the listing"})}

            reviewed_email = listing_item['redeemerEmail']
            if not reviewed_email:
                logger.error("No redeemer found to review")
                return {"statusCode": 400, "body": json.dumps({"error": "No redeemer to review"})}

        reviewed_response = users_table.get_item(Key={'userEmail': reviewed_email})
        if 'Item' not in reviewed_response:
            logger.error("Reviewed user not found")
            return {"statusCode": 404, "body": json.dumps({"error": "Reviewed user not found"})}
        reviewed_item = reviewed_response['Item']

        review = {
            'message': message,
            'rating': rating,
            'writerEmail': writer_email,
            'writerName': writer_item['name']
            
        }

        notification_message = f"User {writer_item['name']} reviewed you."

        notification = { 
            'message': notification_message, 
            'redirect': '/account'
        }

        reviews = reviewed_item['reviews']
        logger.info(f"Reviews: {reviews}")
        ratings = [review['rating'] for review in reviews]
        logger.info(f"Ratings: {ratings}")
        average_rating = (sum(ratings) + rating) / (len(ratings) + 1) if ratings else rating
        average_rating = round(average_rating, 1)
        logger.info(f"Average rating: {average_rating}")

        users_table.update_item(
            Key={'userEmail': reviewed_email},
            UpdateExpression="SET reviews = list_append(if_not_exists(reviews, :empty_list), :r), notifications = list_append(if_not_exists(notifications, :empty_list), :l), averageRating = :a",
            ExpressionAttributeValues={":l": [notification], ":empty_list": [], ":r": [review], ":a": average_rating},
            ReturnValues="UPDATED_NEW"
        )

        if order_exists:
            if is_redeemer:
                update_order = orders_table.update_item(
                    Key={'orderID': order_id},
                    UpdateExpression="SET sellerReviewed = :val",
                    ExpressionAttributeValues={":val": True},
                    ReturnValues="UPDATED_NEW"
                )
                if order_item['redeemerReviewed'] == True:
                    listings_table.update_item(
                        Key={'listingID': order_item['listingID']},
                        UpdateExpression="SET #status = :s",
                        ExpressionAttributeNames={
                            "#status": "status"
                        },
                        ExpressionAttributeValues={":s": "complete"},
                        ReturnValues="UPDATED_NEW"
                    )
                    

            elif is_seller:
                update_order = orders_table.update_item(
                    Key={'orderID': order_id},
                    UpdateExpression="SET redeemerReviewed = :val",
                    ExpressionAttributeValues={":val": True},
                    ReturnValues="UPDATED_NEW"
                )
                if order_item['sellerReviewed'] == True:
                    listings_table.update_item(
                        Key={'listingID': order_item['listingID']},
                        UpdateExpression="SET #status = :s",
                        ExpressionAttributeNames={
                            "#status": "status"
                        },
                        ExpressionAttributeValues={":s": "complete"},
                        ReturnValues="UPDATED_NEW"
                    )

            else:
                logger.error("Unauthorized access")
        
        else:

            redeemer_user_response = users_table.get_item(Key={'userEmail': listing_item['redeemerEmail']})
            redeemer_user_item = redeemer_user_response['Item']
            redeemer_email = listing_item['redeemerEmail']

            update_expression = "SET #status = :status, redeemerEmail = :empty"
            expression_values = {":status": "available", ":empty": "", ":now": now}
            expression_names = {"#status": "status"}

            if listing_item['type'] == 'auction':
                update_expression += ", bids = :empty_list, listingDate = :now, endDate = :new_end"
                duration = int(listing_item['duration'])
                new_end = (datetime.utcnow() + timedelta(days=duration)).isoformat() + "Z"
                expression_values.update({":empty_list": [], ":new_end": new_end})
            else:
                update_expression += ", endDate = :empty, listingDate = :now"

            listings_table.update_item(
                Key={'listingID': listing_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ExpressionAttributeNames=expression_names
            )

            if listing_id in redeemer_user_item.get('redeemedIDs', []):
                redeemer_user_item['redeemedIDs'].remove(listing_id)
                users_table.update_item(
                    Key={'userEmail': redeemer_email},
                    UpdateExpression="SET redeemedIDs = :r",
                    ExpressionAttributeValues={":r": redeemer_user_item['redeemedIDs']}
                )
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Review sent successfully"
            })
        }

    except Exception as e:
        print("Error sending email:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error",
                "error": str(e)
            })
        }

