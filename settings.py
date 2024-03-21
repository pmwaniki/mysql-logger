import os
import toml

base_dir=os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(base_dir, 'config.toml'),'r') as f:
    config_lines=f.read()
    config=toml.loads(config_lines)



database_config=config['mysql']


rabbitmq_config=config['rabbitmq']

kafka_config=config['kafka']

pc_name=config['settings']['pc_name']

binlog_log_path = config['settings']['binlog_log_path']
upload_log_path = config['settings']['upload_log_path']
