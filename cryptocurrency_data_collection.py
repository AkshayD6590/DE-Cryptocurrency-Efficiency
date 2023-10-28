import json
import requests
import time
import boto3

sqs = boto3.client('sqs','eu-north-1')
queue_url = 'https://sqs.eu-north-1.amazonaws.com/274437815341/SQSQueue_For_CoinGeko_Project'
api_url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=true&locale=en'

cryptocurrency_ids = ['bitcoin', 'ethereum', 'ripple']

# Function to fetch and send cryptocurrency data to SQS
def fetch_and_send_data_to_sqs():
    while True:
        try:
            params = {
                'vs_currency': 'usd',
                'ids': ','.join(cryptocurrency_ids),
            }
            response = requests.get(api_url,params=params)
            data = response.json()
            
            for record in data:
                sqs_message = {
                    'id':record['id'],
                    'symbol':record['symbol'],
                    'name':record['name'],
                    'price':record['current_price']
                }
                sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(sqs_message))
                print(f"Sent data to SQS: {sqs_message}")

        except Exception as e:
            print(f"error fecting data:{e}")

        time.sleep(1)
if __name__ == "__main__":
    fetch_and_send_data_to_sqs()





    
