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
DYNAMODB_ORDER_TABLE = os.environ['DYNAMODB_ORDER_TABLE']

def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        params = event.get("queryStringParameters", {}) or {}
        logger.info("Query parameters: %s", params)

        user_id = params.get("userID")
        order_id = params.get("orderID")

        if not user_id and not order_id:
            logger.error("Missing required parameters: userID or orderID")
            return {"statusCode": 400, "body": json.dumps({"error": "Missing required parameters"})}

        user_table = dynamodb.Table(DYNAMODB_USER_TABLE)
        user_response = user_table.query(
            IndexName='userID-index',
            KeyConditionExpression='userID = :uid',
            ExpressionAttributeValues={':uid': user_id},
        )

        user = user_response['Items'][0]

        logger.info("Fetched user: %s", user)

        order_id = f"order-{order_id}"
        order_table = dynamodb.Table(DYNAMODB_ORDER_TABLE)
        order_response = order_table.get_item(Key={'orderID': order_id})
        if 'Item' not in order_response:
                return {"statusCode": 404, "body": json.dumps({"error": "Order not found"})}
        order = order_response.get('Item')

        logger.info("Fetched order: %s", order)

        if order['sellerEmail'] == user['userEmail']:
            response_data = {
                'awb': order['awb'],
                'expirationDate': order['expirationDate'],
                'orderID': order['orderID'],
                'redeemerReviewed': order['redeemerReviewed'],
                'cost': str(order['cost'])
            }

        elif order['redeemerEmail'] == user['userEmail']:
            response_data = {
                'awb': order['awb'],
                'expirationDate': order['expirationDate'],
                'orderID': order['orderID'],
                'sellerReviewed': order['sellerReviewed'],
                'cost': str(order['cost'])
            }
        else:
            return {"statusCode": 403, "body": json.dumps({"error": "Access denied"})} 
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(response_data)
        }

    except Exception as e:
        logger.error("Error: %s", str(e))
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }