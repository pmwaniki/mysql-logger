import argparse
import sys
import json
import pandas as pd


from kafka import KafkaAdminClient,KafkaConsumer

from src.kv_store import KeyValueStore
from src.snapshot import take_snapshot
from settings import kafka_config

store = KeyValueStore()




def test_kafka_connection():
    try:
        client = KafkaAdminClient(**kafka_config)
        client.close()
    except Exception as e:
        raise Exception(f"Unable to connect to Kafka: {e}")
    sys.stdout.write("Connection to kafka successful\n")



def list_remote_backups():
    client = KafkaAdminClient(**kafka_config)
    topics=client.list_topics()
    backups=[i for i in topics if ("_snapshot" in i) | ("_logs" in i)]
    client.close()
    backups=sorted(backups)
    return backups

def get_dump(snapshot):

    value_desirializer=lambda x:json.loads(x.decode('utf-8'))
    key_desirializer=lambda x:int(x.decode('utf-8'))
    client = KafkaConsumer(snapshot, auto_offset_reset='earliest',
                           enable_auto_commit=False, consumer_timeout_ms=1000,
                           value_deserializer=value_desirializer,
                           key_deserializer=key_desirializer,
                           **kafka_config)
    entry=[]
    for message in client:
        m=message.value
        m['index']=message.key
        entry.append(m)
    client.close()
    # check consistency. ie missing index
    df=pd.DataFrame(entry)
    ordered_events=[]
    try:
        for i in range(df['index'].max()):
            ordered_events.append(df.loc[df['index']==i,'line'].iat[0])
    except KeyError:
        raise Exception("Missing snapshot entries.")
    return ordered_events

def get_logs(logfile):

    value_desirializer=lambda x:json.loads(x.decode('utf-8'))
    client = KafkaConsumer(logfile, auto_offset_reset='earliest',
                           enable_auto_commit=False, consumer_timeout_ms=1000,
                           value_deserializer=value_desirializer,
                           **kafka_config)
    entry=[]
    for message in client:
        entry.append(message.value)
    client.close()
    # check consistency. ie missing index
    df=pd.DataFrame(entry)
    df=df.sort_values(by=['Log_name','Pos'])

    ordered_events=[]
    for f in df['Log_name'].unique():
        start_pos=4
        sub_entry=df.loc[df['Log_name']==f,]
        while start_pos<=sub_entry['Pos'].max():
            try:
                entry=sub_entry.loc[sub_entry['Pos']==start_pos,]
            except KeyError:
                raise Exception("Missing log entries")

            ordered_events.append(entry['Log_entry'].iat[0])
            start_pos=entry['End_log_pos'].iat[0]
    return ordered_events








if __name__=="__main__":
    parser=argparse.ArgumentParser()
    subparser=parser.add_subparsers(dest="command")

    test_kafka=subparser.add_parser("test-kafka",help="Test kafka connection")
    setvariable=subparser.add_parser("setvariable",help="Set variable in key value database")
    showvariables=subparser.add_parser("showvariables",help="Show variables set in key value database")
    showbackups=subparser.add_parser("showbackups",help="Show remote backups")
    getdump=subparser.add_parser("getdump",help="Get MYSql dump from remote server")
    getdump.add_argument("snapshot",help="Name of snashot. List of snapshot can be obtained using showbackups command")

    getbinlog = subparser.add_parser("getbinlog", help="Get MYSql binary log from remote server")
    getbinlog.add_argument("snapshot", help="Name of snashot. List of snapshot can be obtained using showbackups command")

    createsnapshot=subparser.add_parser("createsnapshot",help="Create a new snapshot and upload to server")





    setvariable.add_argument("--key",required=True,help="Variable key")
    setvariable.add_argument("--value", required=True, help="Variable value")





    args=parser.parse_args()


    if args.command=="setvariable":
        store[args.key]=args.value
    elif args.command=="showvariables":
        variables=store.items()
        for k,v in variables:
            print(f'{k}:{v}')
    elif args.command=="showbackups":
        backups=list_remote_backups()
        print(json.dumps(backups,indent=4))
    elif args.command=="getdump":
        dump=get_dump(args.snapshot)
        for i in dump:
            sys.stdout.write(i)
    elif args.command=="getbinlog":
        log=get_logs(args.snapshot)
        for i in log:
            sys.stdout.write(i)
    elif args.command=="createsnapshot":
        print("Taking snapshot")
        take_snapshot()
    elif args.command=="test-kafka":
        test_kafka_connection()



