import logging
import subprocess
import time
from logging.handlers import RotatingFileHandler

import pika
import pymysql

import json

from settings import database_config, rabbitmq_config,binlog_log_path
from src.kv_store import KeyValueStore


key_value_store=KeyValueStore()
broker_url = f'amqp://{rabbitmq_config["user"]}:{rabbitmq_config["password"]}@{rabbitmq_config["host"]}:{rabbitmq_config["port"]}/{rabbitmq_config["virtual_host"]}'



log_topic_key = 'log_topic_name'
last_read_log_key = 'last_read_log'
last_read_event_key = 'last_read_event'

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Adjust logging level as needed
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# rotate_handler = RotatingFileHandler(binlog_log_path, maxBytes=20, backupCount=5)
# logger.addHandler(rotate_handler)




def get_query(query):
    connection = pymysql.connect(**database_config,
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results=cursor.fetchall()
    return results




def get_new_events(last_bin_log,last_event_pos=None):
    binary_logs=get_query("show binary logs")
    binary_log_names=[l['Log_name']  for l in binary_logs ]
    last_binarylog_index=[i  for i,l in enumerate(binary_log_names) if l==last_bin_log][0]
    new_binnary_logs=binary_log_names[last_binarylog_index:]
    new_events=[]
    for i, l in enumerate(new_binnary_logs):
        if (i!=0) | (last_event_pos is None):
            query = f'show binlog events in "{l}"'
            events = get_query(query)
        else:
            query = f'show binlog events in "{l}" from {last_event_pos}'
            events = get_query(query)
            # events=events[1:] # pop the first event since it already captured
        events=sorted(events,key=lambda k: k['Pos'])
        new_events+=events
    return new_events


def read_log_entry(binary_log_file,start_position=None,end_position=None):
    command=["mysqlbinlog",
             "--read-from-remote-server",
             f"--host={database_config['host']}",
             f"--port={database_config['port']}",
             f"--user={database_config['user']}",
             f"--password={database_config['password']}",
             "--skip-gtids",
             f"--start-position={start_position}",
             f"--stop-position={end_position}",
        f"{binary_log_file}"
             ]
    try:
        log_event=subprocess.check_output(command)
    except Exception as e:
        raise Exception(f"Error reading log event: {e}")
    return log_event





def read_binlog():
    log_queue_name=key_value_store[log_topic_key]
    last_read_log = key_value_store[last_read_log_key]
    last_read_event = key_value_store[last_read_event_key]

    try:
        new_events=get_new_events(last_read_log,last_event_pos=None if last_read_event=="-1" else int(last_read_event))
        if len(new_events)==0:
            return None

    except Exception as e:
        logger.error(f"Unable to retrieve event list from mysql: {e}")
        raise Exception("Unable to retrieve new events from mysql")

    #query event should end with Xid events
    new_events2=[]
    in_query=False
    proxy_event={}
    for i,new_event in enumerate(new_events):
        if not in_query:
            if new_event['Event_type'] in ['Rows_query','Table_map','Update_rows']:
                logger.error("Transaction events outsize in_query")
                raise Exception("Transaction events outsize in_query")
            if new_event["Event_type"] != "Query":
                new_events2.append(new_event)
                continue
            else:
                in_query= True
                proxy_event={'Log_name': new_event['Log_name'], 'Pos': new_event["Pos"],
                             'Event_type': 'Combined_Query', 'Server_id': new_event["Server_id"],
                             'End_log_pos': None, 'Info': "Combined query event"}
                continue
        else:
            if new_event["Event_type"] == "Xid":
                in_query=False
                proxy_event['End_log_pos']=new_event['End_log_pos']
                new_events2.append(proxy_event)



    """Reads MySQL binary log, writes events to Redis, tracks read log and event, with logging."""

    logger.info("Starting to read MySQL binary log...")

    try:
        connection = pika.BlockingConnection(pika.URLParameters(broker_url))
        channel = connection.channel()
        channel.queue_declare(queue='binarylogs', durable=True)
        for event in new_events2:
            # Process the event and create a dictionary
            event_data = {
                'topic_name': log_queue_name,
                'key':f"{event['Log_name']}-{event['Pos']}-{event['End_log_pos']}",
                'Log_name': event['Log_name'],
                'Pos': event['Pos'],
                'Event_type':event['Event_type'],
                'End_log_pos':event['End_log_pos'],
                'Log_entry':read_log_entry(binary_log_file=event['Log_name'],start_position=event['Pos'],end_position=event['End_log_pos']).decode('utf-8')
            }

            channel.basic_publish(exchange='', routing_key='binarylogs',
                                  body=json.dumps(event_data),
                                  properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                                  )
            logger.info(f"Log event written to rabbitmq file:{event['Log_name']} pos: {event['Pos']}")



            # Update last read log and event
            key_value_store[last_read_log_key]=event['Log_name']
            key_value_store[last_read_event_key]=event['End_log_pos']

    except Exception as e:
        logger.error(f"Error reading binary log: {e}")
        raise Exception(f"Error reading binary log: {e}")


    logger.info("Finished reading binary log, scheduled for retry in 5 seconds.")

# Start the Celery worker
if __name__ == '__main__':
    while True:
        try:
            read_binlog()
            time.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            time.sleep(5)
            continue