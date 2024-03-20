import os

base_dir=os.path.dirname(os.path.realpath(__file__))

def get_env_or_raise(env_var):
    v=os.environ.get(env_var, None)
    if v is None: raise Exception(f"Environment variable '{env_var}' not set.")
    return v

server_id = 1  # Your MySQL server ID

database_config={
    'host': os.environ.get('DATABASE_HOST', '127.0.0.1'),
    'port': int(os.environ.get('DATABASE_PORT','3306')),
    'user':get_env_or_raise('DATABASE_USERNAME'),
    'password': get_env_or_raise('DATABASE_PASSWORD')
}


rabbitmq_config={
    'user':get_env_or_raise('RABBITMQ_USERNAME'),
    'password':get_env_or_raise('RABBITMQ_PASSWORD'),
    'host':'127.0.0.1',
    'port':5672,
    'virtual_host':'replication-vhost'
}

kafka_config={
    'bootstrap_servers':'hsulogs.kemri-wellcome.org:9092',
    'security_protocol':'SSL',
    'ssl_keyfile':'/etc/ssl/kafka/client-key.pem',
    'ssl_certfile':'/etc/ssl/kafka/client-cert.pem'
}

pc_name=get_env_or_raise('PC_NAME')

snapshot_log_path = '/tmp/mysql-logger-snapshot.log'
upload_log_path = '/tmp/mysql-logger-upload.log'
