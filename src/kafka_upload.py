import json
import logging
from functools import partial
import time
from logging.handlers import RotatingFileHandler

import pika

from kafka import KafkaProducer

from settings import rabbitmq_config, kafka_config, upload_log_path

broker_url = f'amqp://{rabbitmq_config["user"]}:{rabbitmq_config["password"]}@{rabbitmq_config["host"]}:{rabbitmq_config["port"]}/{rabbitmq_config["virtual_host"]}'


# Logging configuration

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Adjust logging level as needed
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
rotate_handler = RotatingFileHandler(upload_log_path, maxBytes=20, backupCount=5)
logger.addHandler(rotate_handler)


def publish_message(producer, topic_name,value, key):
    try:
        key_bytes = bytes(str(key), encoding='utf-8')
        value_bytes = json.dumps(value).encode('utf-8')
        producer.send(topic_name, key=key_bytes, value=value_bytes)
        producer.flush()
    except Exception as ex:
        raise Exception("Unable to publish to kafka")

def upload_snapshot():
    logger.info("Attempting to connect to kafka ..... ")
    #open kafka producer
    try:
        producer = KafkaProducer(**kafka_config,retries=0,acks="all")
        logger.info("Connected to kafka ....")
    except Exception as e:
        logger.error(f"Kafka unavailable: {e}")
        raise Exception("Kafka unavailable")
    # rabbitmq consumer
    connection=pika.BlockingConnection(pika.URLParameters(broker_url))
    channel=connection.channel()




    channel.queue_declare('snapshot',durable=True)
    channel.queue_declare('binarylogs', durable=True)



    def mq_callback(ch, method, properties, body,producer):

        # def callback_success(record_metadata,ch):
        #
        #     ch.basic_ack(delivery_tag=method.delivery_tag)
        #     logger.info(f"Message from queue '{record_metadata.topic}' delivered and acknowledged.")
        #
        # def callback_error(e,ch):
        #     logger.error("Kafka error", exc_info=e)
        #     ch.basic_nack(delivery_tag=method.delivery_tag)
        #     raise Exception(f"Kafka error: {e}")
        message=json.loads(body)
        queue_name=message.pop('topic_name')
        key=message.pop('key')
        try:
            publish_message(producer=producer,topic_name=queue_name,value=message,key=key)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Message '{key}' delivered and acknowledged.")
        except Exception as e:
            logger.error("Kafka error", exc_info=e)
            ch.basic_nack(delivery_tag=method.delivery_tag)
            raise Exception(f"Kafka error: {e}")


        # (producer.send(queue_name,json.dumps(message).encode('utf-8')).
        #  add_callback(partial(callback_success,ch=ch)).
        #  add_errback(partial(callback_error,ch=ch)))


    channel.basic_consume(queue='snapshot',on_message_callback=partial(mq_callback,producer=producer))
    channel.basic_consume(queue='binarylogs',on_message_callback=partial(mq_callback,producer=producer))
    # channel.basic_qos(prefetch_count=1)
    channel.start_consuming()

    connection.close()
    producer.close()





if __name__ == "__main__":
    while True:
        try:

            upload_snapshot()
            time.sleep(10)
        except KeyboardInterrupt:
            break
        except Exception as e:
            time.sleep(5)
            continue