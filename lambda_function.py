import json
import boto3
import redis

sqs = boto3.client('sqs', region_name='eu-north-1')
redis_client = redis.Redis(host='rediscluster-coingeko-project.myvpoo.clustercfg.eun1.cache.amazonaws.com', port=6379, db=0)


def lambda_handler(event, context):
    # Read messages from the SQS queue
    for record in event['Records']:
        message = json.loads(record['body'])
        
        # Perform data processing and transformation
        processed_data = process_message(message)
        
        # Store the processed data, e.g., send it to another AWS service
        # Or, send it to another SQS queue for further processing
        send_processed_data_to_another_service(processed_data)
    
def process_message(message):
    # Define a list of symbols for the cryptocurrencies of interest (Bitcoin, Ethereum, and Ripple).
    # You can modify this list to include additional symbols or remove existing ones.
    cryptocurrency_symbols = ["btc", "eth", "xrp"]
    
    # Parse the message JSON
    try:
        data = json.loads(message)
    except Exception as e:
        print(f"Failed to parse the message JSON: {e}")
        return None

    # Check if the "symbol" field in the message data is in the list of cryptocurrency symbols.
    # If it matches, keep the record; otherwise, discard it.
    if data.get("symbol") in cryptocurrency_symbols:
        # If it's one of the cryptocurrencies of interest, return the processed data.
        return data
    else:
        # If it's not one of the cryptocurrencies of interest, return None to indicate that it should be discarded.
        return None

def send_processed_data_to_another_service(data):
    if data:
        # Convert the data to JSON format (string).
        data_json = json.dumps(data)

        try:
            # Send the JSON data to Redis, for example, using a specific key.
            redis_key = 'akshay-lambda-key'
            redis_client.set(redis_key, data_json)
            print(f"Data sent to Redis with key: {redis_key}")
        except Exception as e:
            print(f"Failed to send data to Redis: {e}")
    else:
        print("Data is None, nothing to send.")
