import json
import subprocess
import os
import tempfile

import pika
# from src.crud import set_variable


import pymysql
from datetime import datetime
from settings import database_config,pc_name,rabbitmq_config
from src.kv_store import KeyValueStore


key_value_store=KeyValueStore()
broker_url = f'amqp://{rabbitmq_config["user"]}:{rabbitmq_config["password"]}@{rabbitmq_config["host"]}:{rabbitmq_config["port"]}/{rabbitmq_config["virtual_host"]}'




def create_initial_dump(temp_file):
    try:
        output = subprocess.check_output(["mysqldump", f"--host={database_config['host']}",
                                          f"--port={database_config['port']}", f"--user={database_config['user']}",
                                          f"--password={database_config['password']}",
                                          "--skip-extended-insert", "--all-databases",
                                          "--events", "--routines",
                                          "--master-data=2",
                                          "--flush-logs",
                                          "--lock-all-tables"])
        connection = pymysql.connect(host=database_config['host'],
                                     user=database_config['user'],
                                     port=int(database_config['port']),
                                     password=database_config['password'],
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            cursor.execute("show binary logs;")
            results = cursor.fetchall()
            current_logfile = results[-1]['Log_name']
        with open(temp_file,'w') as f:
            f.write(output.decode("utf8"))
    except Exception as e:
        raise Exception(f"Failed to create initial dump:{e}")
    return current_logfile

def parse_mysql_dump(dump_file_path):
    """Parses a MySQL dump file and extracts SQL statements.

    Args:
        dump_file_path (str): Path to the MySQL dump file.

    Yields:
        str: Each extracted SQL statement.
    """

    try:
        with open(dump_file_path, "r") as dump_file:

            for i,line in enumerate(dump_file):
                yield line

    except FileNotFoundError as e:
        print(f"Error: Dump file not found: {dump_file_path}")
    except Exception as e:
        print(f"Error parsing dump file: {e}")

def take_snapshot():
    snapshot_topic_name=f'{pc_name}_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}_snapshot'
    log_topic_name=snapshot_topic_name.replace("snapshot",'logs')
    temp_file = tempfile.mktemp(suffix=".sql")
    current_logfile=create_initial_dump(temp_file=temp_file)


    connection=pika.BlockingConnection(pika.URLParameters(broker_url))
    channel=connection.channel()
    channel.queue_declare(queue='snapshot',durable=True)
    try:

        for i,line in enumerate(parse_mysql_dump(temp_file)):#pass
            channel.basic_publish(exchange='',routing_key='snapshot',
                                  body=json.dumps({'topic_name':snapshot_topic_name,
                                        'key':i,
                                        'line':line}),
                                  properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
                                  )
            # print("this")
    except Exception as e:
        raise Exception(f"Unable to write to rabbitmq queue:{e}")
    finally:
        connection.close()
    key_value_store['snapshot_topic_name']=snapshot_topic_name
    key_value_store['log_topic_name']=log_topic_name
    key_value_store['last_read_log']=current_logfile
    key_value_store['last_read_event']=-1
    os.remove(temp_file)




if __name__=="__main__":
    take_snapshot()