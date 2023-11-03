import boto3
import json

from dotenv import dotenv_values

config = dotenv_values()

client = boto3.client('dynamodb', region_name=config.get('AWS_DEFAULT_REGION'),
                      aws_access_key_id=config.get('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key=config.get('AWS_SECRET_ACCESS_KEY'))
with open("student_data.json") as fp:
    users = json.load(fp)

for user in users:
    print(user)
    response = client.put_item(
        TableName='student_table',
        Item={
            "id": {"N": str(user["id"])},
            "name": {"S": user["name"]},
            "major": {"S": user["major"]},
            "year": {"S": user["year"]},
    })

print("Data uploaded")