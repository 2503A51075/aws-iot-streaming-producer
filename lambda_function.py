import base64
import json
import os
import time
import boto3

s3_client = boto3.client('s3')
BUCKET_NAME = 'sensor-stream-data-2503a51075'

def lambda_handler(event, context):
    records_to_save = []
    
    for record in event['Records']:
        # Decode the incoming base64 Kinesis payload
        payload_bytes = base64.b64decode(record['kinesis']['data'])
        payload_json = json.loads(payload_bytes.decode('utf-8'))
        
        # Add processing timestamp
        payload_json['processed_at'] = int(time.time())
        records_to_save.append(payload_json)
    
    if records_to_save:
        file_name = f"sensor_batch_{int(time.time())}_{context.aws_request_id[:8]}.json"
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=f"incoming-data/{file_name}",
            Body=json.dumps(records_to_save, indent=2),
            ContentType='application/json'
        )
        print(f"Stored {len(records_to_save)} records into S3 bucket: {BUCKET_NAME}")

    return {'statusCode': 200, 'body': f"Processed {len(records_to_save)} records."}