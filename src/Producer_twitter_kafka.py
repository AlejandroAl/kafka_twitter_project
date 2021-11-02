from config_helper import Config_reader_helper
import socket
import json
from confluent_kafka import Producer
from TwitterAPI import TwitterAPI
from TwitterAPI import TwitterRequestError, TwitterConnectionError

helper_config = Config_reader_helper()

# kafka configuration propeties
topic_name = helper_config.get_property_kafka("topic.name")
conf = {'bootstrap.servers': helper_config.get_property_kafka("bootstrap.servers"),
        'client.id': socket.gethostname()}

producer = Producer(conf)

# create api instance to get data from Twitter
consumer_key, consumer_secret, access_token_key, access_token_secret = helper_config.get_twitter_preperties()
track_value_filter = helper_config.get_property_kafka("track.search")
api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

while True:
    try:
        iterator = api.request('statuses/filter', {'track':track_value_filter}).get_iterator()
        for item in iterator:
            if 'text' in item:
                message = {}
                message["created_at"] = item.get('created_at')
                message["id"] = item.get('id')
                message["text"] = item.get('text')
                message["user_id"] = item.get('user').get("id")
                message["user_name"] = item.get('user').get("name")
                message["user_location"] = item.get('user').get("location")
                message_to_kafka = json.dumps(message)
                print("sending message", message_to_kafka)
                producer.produce(topic_name, message_to_kafka)
                print("message sent")
                print("#"*30)
            elif 'disconnect' in item:
                event = item['disconnect']
                if event['code'] in [2,5,6,7]:
                    # something needs to be fixed before re-connecting
                    raise Exception(event['reason'])
                else:
                    # temporary interruption, re-try request
                    break
    except TwitterRequestError as e:
        if e.status_code < 500:
            # something needs to be fixed before re-connecting
            raise
        else:
            # temporary interruption, re-try request
            pass
    except TwitterConnectionError:
        # temporary interruption, re-try request
        pass
