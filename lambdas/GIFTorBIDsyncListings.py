import json
import logging
import boto3
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
DYNAMODB_WEBSOCKET_TABLE = os.environ['DYNAMODB_WEBSOCKET_TABLE']
ENDPOINT_URL = os.environ['ENDPOINT_URL']
api_client = boto3.client('apigatewaymanagementapi', endpoint_url=ENDPOINT_URL)

def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event))
    records = event.get('Records', [])

    for record in records:
        if record['eventName'] in ['INSERT', 'MODIFY']:
            new_image = record['dynamodb'].get('NewImage', {})
            old_image = record['dynamodb'].get('OldImage', {})
            listing_id = new_image.get('listingID', {})
            listing_type = new_image.get('type', {})
            new_status = new_image.get('status', {})
            old_status = old_image.get('status', {})
            if has_significant_change(new_image, old_image):
                    notify_clients(listing_id.get('S', ''), listing_type.get('S', ''))

def has_significant_change(new_image, old_image):
    new_status = new_image.get('status', {}).get('S')
    old_status = old_image.get('status', {}).get('S')
    if new_status != old_status and old_status in ['available', 'redeemed']:
        return True

    if 'bids' in new_image and 'bids' in old_image:
        new_bids = new_image.get('bids', {}).get('L', [])
        old_bids = old_image.get('bids', {}).get('L', [])
        if new_bids != old_bids:
            return True

    return False

def get_all_connection_ids():
    connections_table = dynamodb.Table(DYNAMODB_WEBSOCKET_TABLE)
    connection_ids = []
    response = connections_table.scan(ProjectionExpression='connectionID')
    for item in response['Items']:
        connection_ids.append(item['connectionID'])
    return connection_ids

def notify_clients(listing_id, listing_type):
    connection_ids = get_all_connection_ids()
    for connection_id in connection_ids:
        send_message_to_client(connection_id, listing_id, listing_type)

def send_message_to_client(connection_id, listing_id, listing_type):
    try:
        api_client.post_to_connection(ConnectionId=connection_id, Data=json.dumps({"message": "Updade for listing", "listing": listing_id, "type": listing_type }))
    except Exception as e:
        logger.error("Failed to send message to %s: %s", connection_id, str(e))

