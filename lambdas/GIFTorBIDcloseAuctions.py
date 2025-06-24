import json
import boto3
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

DYNAMODB_LISTING_TABLE = os.environ['DYNAMODB_LISTING_TABLE']
DYNAMODB_USER_TABLE = os.environ['DYNAMODB_USER_TABLE']

def lambda_handler(event, context):
    now = datetime.utcnow().isoformat() + "Z"

    listings_table = dynamodb.Table(DYNAMODB_LISTING_TABLE)

    listings_response = listings_table.scan(
        FilterExpression='#type = :t AND endDate <= :ed AND #status = :s',
        ExpressionAttributeNames={
            '#status': 'status',
            '#type': 'type'
        },
        ExpressionAttributeValues={
            ':t': 'auction',
            ':ed': now,
            ':s': 'available'
        }
    )

    logger.info("Found %s matching listings %s:", listings_response.get('Count', 0), listings_response)

    expired_auctions = listings_response.get('Items', [])

    for listing in expired_auctions:
        listing_id = listing['listingID']
        name = listing['name']
        seller_email = listing['sellerEmail']
        duration = int(listing['duration'])
        bids = listing['bids']

        users_table = dynamodb.Table(DYNAMODB_USER_TABLE)

        if len(bids) > 0:
            logger.info("Found bids for listing %s:", listing_id)

            redeemer_email = bids[0]['bidderEmail']
            listings_table.update_item(
                Key={'listingID': listing_id},
                UpdateExpression='SET #status = :s, redeemerEmail = :re',
                ExpressionAttributeNames={
                    "#status": "status"
                },
                ExpressionAttributeValues={
                    ':s': 'redeemed',
                    ':re': redeemer_email
                }
            )

            logger.info("Updated listing %s to redeemed.", listing_id)

            seller_message = f"Your auction {name} has ended."
            seller_notification = {
                'message': seller_message,
                'redirect': 'posts'
            }
            
            redeemer_message = f"You have won the auction for {name}."
            redeemer_notification = {
                'message': redeemer_message,
                'redirect': '/aquisitions'
            }

            logger.info("Created notifications for seller %s and redeemer %s.", seller_email, redeemer_email)

            users_table.update_item(
                Key={'userEmail': seller_email},
                UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :l)",
                ExpressionAttributeValues={":l": [seller_notification], ":empty_list": []}
            )

            logger.info("Updated notifications for seller %s.", seller_email)

            users_table.update_item(
                Key={'userEmail': redeemer_email},
                UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :n), redeemedIDs = list_append(if_not_exists(redeemedIDs, :empty_list), :l)",
                ExpressionAttributeValues={":n": [redeemer_notification], ":empty_list": [], ":l": [listing_id]}
            )

            logger.info("Updated notifications for redeemer %s.", redeemer_email)

        else:
            logger.info("No bids found for listing %s.", listing_id)

            listing_date = datetime.utcnow().isoformat() + "Z"
            end_date = datetime.utcnow() + timedelta(days=duration)
            listings_table.update_item(
                Key={'listingID': listing_id},
                UpdateExpression='SET listingDate = :ld, endDate = :ed',
                ExpressionAttributeValues={
                    ':ld': listing_date,
                    ':ed': end_date.isoformat() + "Z"
                }
            )

            logger.info("Updated listing %s to renewed.", listing_id)

            seller_message = f"Auction {name} has ended and have been automatically renewed."
            seller_notification = {
                'message': seller_message,
                'redirect': '/posts'
            }

            logger.info("Created notification for seller %s.", seller_email)

            users_table.update_item(
                Key={'userEmail': seller_email},
                UpdateExpression="SET notifications = list_append(if_not_exists(notifications, :empty_list), :l)",
                ExpressionAttributeValues={":l": [seller_notification], ":empty_list": []}
            )

            logger.info("Updated notifications for seller %s.", seller_email)


    return {
        'statusCode': 200,
        'body': json.dumps('Finished processing expired auctions.')
    }
