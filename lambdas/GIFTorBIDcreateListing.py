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
        logger.info("Sub: %s", sub)
        seller_email = body.get('sellerEmail')
        logger.info("sellerEmail: %s", seller_email)
        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        response = user_table.get_item(Key={'userEmail': seller_email})
        logger.info("Response1: %s", response)

        if 'Item' not in response or response['Item']['userID'] != sub:
            logger.error("Unauthorized or no such user")
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized or no such user"})}

        user_item = response['Item']

        logger.info("Passed: check if user exists and sub matches")

        required_attributes = ['name', 'type', 'category', 'description', 'sellerEmail', 'images']
        for attribute in required_attributes:
            if attribute not in body:
                logger.error(f"Missing required attribute: {attribute}")
                return {"statusCode": 400, "body": json.dumps({"error": f"Missing required attribute: {attribute}"})}

        logger.info("Passed: validate required attributes")

        listing_id = f"{body['type'].lower()}-{str(uuid.uuid4())}"
        logger.info("ListingID: %s", listing_id)

        image_urls = process_images(body['images'], listing_id, body['type'])
        logger.info("Images urls: %s", image_urls)

        item = create_listing_item(body, listing_id, image_urls, user_item['name'])

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        response = listing_table.put_item(Item=item)
        logger.info("DynamoDb resonse: %s", response)

        update_user_listings(seller_email, listing_id)

        return {
            "statusCode": 201,
            "body": json.dumps({
                "message": f"{body['type']} created successfully!",
                "listingID": listing_id
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def process_images(images, object_id, type):
    folder = "donations" if type.lower() == "donation" else "auctions"
    image_urls = []
    for index, image in enumerate(images):
        image_data = base64.b64decode(image.split(",")[1])  
        s3_key = f"{folder}/{object_id}-{index + 1}.jpg"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=image_data, ContentType="image/jpeg")
        image_urls.append(f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}")
    return image_urls

def create_listing_item(body, object_id, image_urls, user_name):
    listing_date = datetime.utcnow().isoformat() + "Z"
    item = {
        "listingID": object_id,
        "status": "available",
        "name": body['name'],
        "type": body['type'],
        "category": body['category'],
        "description": body['description'],
        "sellerEmail": body['sellerEmail'],
        "images": image_urls,
        "redeemerEmail": "",
        "listingDate": listing_date,
        "sellerName": user_name
    }
    if body['type'].lower() == "auction":
        duration = int(body.get("duration", 7))
        end_date = datetime.utcnow() + timedelta(days=duration)
        item.update({
            "bids": [],
            "duration": duration,
            "endDate": end_date.isoformat() + "Z"
        })
    if body['type'].lower() == "donation":
        item.update({
            "endDate": ""
        })
    return item

def update_user_listings(user_email, listing_id):
    user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
    user_table.update_item(
        Key={'userEmail': user_email},
        UpdateExpression='SET listingsIDs = list_append(listingsIDs, :val)',
        ExpressionAttributeValues={':val': [listing_id]}
    )
