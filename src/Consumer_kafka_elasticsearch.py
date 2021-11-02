import socket
import json
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from config_helper import Config_reader_helper

helper_config = Config_reader_helper()

# kafka configration
topic_name = helper_config.get_property_kafka("topic.name")
conf = {'bootstrap.servers': helper_config.get_property_kafka("bootstrap.servers"),
        'group.id': helper_config.get_property_kafka("group.id"),
        'client.id': socket.gethostname()}
consumer = Consumer(conf)

# Start elasticsearch connecction
es = Elasticsearch(helper_config.get_property_elasticsearch('host'))

running = True

def msg_process(msg, es):

    message_twitter = json.loads(((msg.value())))

    doc = {
    'created_at': message_twitter.get('created_at'),
    'id': message_twitter.get('id'),
    'text': message_twitter.get('text'),
    'user_id': message_twitter.get('user_id'),
    'user_name': message_twitter.get('user_name'),
    'user_location': message_twitter.get('user_location')}

    print("sending message to index" + str(helper_config.get_property_elasticsearch('index')))
    print(message_twitter)
    res = es.index(index=helper_config.get_property_elasticsearch('index'), id=message_twitter.get('id'), document=doc)
    print("-------result---------")
    print(res['result'])
    print("----------------------")
    print("#"*30)

def basic_consume_loop(consumer, topics, es):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg, es)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, [topic_name], es)
