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

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        if 'body' not in event:
            logger.error("Missing 'body' in the event")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing 'body' in the event"})}

        body = json.loads(event['body'])
        user_id = body.get('userID')
        country = body.get('country')
        county = body.get('county')
        city = body.get('city')
        address = body.get('address')
        postal_code = body.get('postalCode')

        if not user_id:
            logger.error("Missing required parameter userID")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameter userID"})}

        table = dynamodb.Table(DYNAMODB_USER_TABLE)
        response = table.query(
            IndexName='userID-index',
            KeyConditionExpression='userID = :uid',
            ExpressionAttributeValues={':uid': user_id},
        )

        if not response.get('Items'):
            logger.error(f"User not found: {user_id}")
            return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

        user_email = response['Items'][0]['userEmail'] 

        update_response = table.update_item(
            Key={'userEmail': user_email},
            UpdateExpression="SET country = :c, county = :co, city = :ci, address = :a, postalCode = :pc",
            ExpressionAttributeValues={
                ':c': country,
                ':co': county,
                ':ci': city,
                ':a': address,
                ':pc': postal_code
            },
            ReturnValues="UPDATED_NEW"
        )
        
        logger.info("User updated successfully: %s", update_response)

        return {"statusCode": 200, "body": json.dumps({"message": "User update successfully"})}

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

