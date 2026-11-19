import boto3
import json
import time
import random

REGION = "us-east-1"
STREAM_NAME = "sensor-data-stream"

kinesis = boto3.client('kinesis', region_name=REGION)

print("Starting Virtual OLED Potentiometer Producer...")

while True:
    val = round(random.uniform(10.0, 95.0), 2)
    status = "NORMAL (MEDIUM)"
    if val < 35: status = "NORMAL (LOW)"
    if val > 75: status = "WARNING (HIGH)"
    
    payload = {
        "device_id": "SIMULATED_ESP32_01",
        "sensor_value": val,
        "status": status,
        "timestamp": int(time.time())
    }
    
    print(f"\n[ VIRTUAL OLED DISPLAY ]")
    print(f"------------------------")
    print(f" Reading : {val} %")
    print(f" Status  : {status}")
    print(f"------------------------")
    
    try:
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(payload),
            PartitionKey="sensor-partition-1"
        )
        print(f"Record Sent | SequenceNo: {response['SequenceNumber'][:20]}...")
    except Exception as e:
        print(f"Error sending payload: {e}")
        
    time.sleep(2)