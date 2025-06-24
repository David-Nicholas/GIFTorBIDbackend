import json
import boto3
import os

REGION_NAME = os.environ['REGION_NAME']
SUPPORT_EMAIL = os.environ['SUPPORT_EMAIL']

ses = boto3.client("ses", region_name=REGION_NAME)  

def lambda_handler(event, context):
    try:
       
        name = event.get("name")
        email = event.get("email")
        subject = event.get("subject")
        body_text = event.get("bodyText")

        if not all([name, email, subject, body_text]):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing required fields"})
            }

       
        email_content = f"""
        Name: {name}
        Email: {email}

        Subject: {subject}

        Message:
        {body_text}
        """

        
        response = ses.send_email(
            Destination={
                "ToAddresses": [SUPPORT_EMAIL]
            },
            Message={
                "Body": {
                    "Text": {"Data": email_content}  
                },
                "Subject": {
                    "Data": "New Message Received"  
                },
            },
            Source=SUPPORT_EMAIL,  
        )

        print("Email sent successfully:", response)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Email sent successfully",
                "response": response
            })
        }

    except Exception as e:
        print("Error sending email:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Failed to send email",
                "error": str(e)
            })
        }
