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
        seller_email = body.get('sellerEmail')
        listing_id = body.get('listingID')

        if not sub or not seller_email or not listing_id:
            logger.error("Missing required parameters: sub, sellerEmail, or listingID")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        user_response = user_table.get_item(Key={'userEmail': seller_email})
        if 'Item' not in user_response:
            logger.error(f"User not found: {seller_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        user_item = user_response['Item']
        if user_item['userID'] != sub or listing_id not in user_item.get('listingsIDs', []):
            logger.error("Unauthorized access or listing not found")
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access or listing not found"})}

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        listing_response = listing_table.get_item(Key={'listingID': listing_id})
        if 'Item' not in listing_response:
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}

        logger.info("Listing found: %s", listing_response['Item'].get('images', []))
        if 'images' in body and body['images']:
            delete_images(listing_response['Item'].get('images', []))
            new_image_urls = upload_new_images(body['images'], listing_id, listing_response['Item'].get('type'))
            update_listing(listing_id, body, listing_table, new_image_urls)
        else:
            update_listing(listing_id, body, listing_table, listing_response['Item'].get('images', []))

        return {"statusCode": 200, "body": json.dumps({"message": "Listing updated successfully"})}

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def delete_images(image_urls):
    for url in image_urls:
        key = '/'.join(url.split("https://")[1].split("/")[1:])
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
        logger.info(f"Deleted image: {key}, with the url: {url}")

def upload_new_images(images, object_id, type):
    folder = "donations" if type.lower() == "donation" else "auctions"
    image_urls = []
    for index, image in enumerate(images):
        image_data = base64.b64decode(image.split(",")[1])
        s3_key = f"{folder}/{object_id}-{index + 1}.jpg"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=image_data, ContentType="image/jpeg")
        image_urls.append(f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}")
    return image_urls

def update_listing(listing_id, body, listing_table, image_urls):
    expression_attribute_names = {"#name": "name", "#desc": "description", "#images": "images"}
    expression_attribute_values = {
        ":name": body.get("name"),
        ":desc": body.get("description"),
        ":images": image_urls
    }
    update_expression = "SET #name = :name, #desc = :desc, #images = :images"
    listing_table.update_item(
        Key={'listingID': listing_id},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )
