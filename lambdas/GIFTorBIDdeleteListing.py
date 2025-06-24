import json
import boto3
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
        logger.info("Parsed body: %s", body)

        sub = body.get('sub')
        seller_email = body.get('sellerEmail')
        listing_id = body.get('listingID')

        if not sub or not seller_email or not listing_id:
            logger.error("Missing required parameters: sub, sellerEmail, or listingID")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        user_response = user_table.get_item(Key={'userEmail': seller_email})

        if 'Item' not in user_response:
            logger.error(f"User not found in database: {seller_email}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        user_item = user_response['Item']

        if 'userID' not in user_item or user_item['userID'] != sub:
            logger.error(f"Unauthorized access attempt by user: {sub}")
            return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized access"})}

        if 'listingsIDs' not in user_item or listing_id not in user_item['listingsIDs']:
            logger.error(f"Listing ID not found under user's listings: {listing_id}")
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found or unauthorized"})}

        listing_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)
        listing_response = listing_table.get_item(Key={'listingID': listing_id})

        if 'Item' not in listing_response:
            logger.error(f"Listing not found in database: {listing_id}")
            return {"statusCode": 404, "body": json.dumps({"error": "Listing not found"})}

        listing_item = listing_response['Item']
        current_images = listing_item.get('images', [])

        delete_images(current_images)

        listing_table.delete_item(Key={'listingID': listing_id})

        update_user_listings(seller_email, listing_id, user_table)

        logger.info(f"Successfully deleted listing: {listing_id}")
        return {"statusCode": 200, "body": json.dumps({"message": "Listing deleted successfully"})}

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

def delete_images(image_urls):
    """Delete images from S3 based on their URLs."""
    try:
        for url in image_urls:
            key = url.split(f"https://{S3_BUCKET}.s3.amazonaws.com/")[-1]
            s3.delete_object(Bucket=S3_BUCKET, Key=key)
            logger.info(f"Deleted image from S3: {key}")
    except Exception as e:
        logger.error(f"Error deleting images from S3: {str(e)}")

def update_user_listings(seller_email, listing_id, user_table):
    """Remove the listingID from the user's listingsIDs array in DynamoDB."""
    try:
        response = user_table.get_item(Key={'userEmail': seller_email})

        if 'Item' not in response:
            logger.error(f"User not found in database: {seller_email}")
            return

        user_item = response['Item']
        listing_ids = user_item.get('listingsIDs', [])

        if listing_id not in listing_ids:
            logger.warning(f"Listing ID {listing_id} not found in user's listings")
            return

        listing_ids.remove(listing_id)

        user_table.update_item(
            Key={'userEmail': seller_email},
            UpdateExpression="SET listingsIDs = :updated_list",
            ExpressionAttributeValues={":updated_list": listing_ids}
        )

        logger.info(f"Successfully removed {listing_id} from user's listingsIDs: {seller_email}")

    except Exception as e:
        logger.error(f"Error updating user's listingsIDs: {str(e)}")

