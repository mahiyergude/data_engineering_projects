import boto3
import json  # Needed for payload handling

# 1. Create a Lambda client
config = boto3.session.Config(
        connect_timeout=900, 
        read_timeout= 900, 
        retries={'max_attempts': 0} # Configure retries.
    )

lambda_client = boto3.client('lambda', config=config)

# 2. Define parameters for invoke()
function_name = 'get_api_weather_data_lambda'  # Placeholder!
invocation_type = 'RequestResponse'
payload_data = {
                "weather_data_type_extraction": "actual",
                "api_token": "8b8c4f3c02be4c40964170107252302",
                "batch_id": 0} # Example payload

# 3. Invoke the function
response = lambda_client.invoke(
    FunctionName=function_name,
    InvocationType=invocation_type,
    Payload=json.dumps(payload_data) # Important: Serialize payload to JSON string!
)

#return
json.loads(response["Payload"].read().decode("utf-8"))