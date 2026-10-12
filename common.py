
#shared settings for the sensors and detectors

from confluent_kafka.admin import AdminClient, NewTopic

Server = "localhost:9092"   
Topic = "security_logs"        
Partitions = 4                 
def topic_check():

    admin = AdminClient({"bootstrap.servers": Server}) 
    if Topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(Topic, num_partitions=Partitions, replication_factor=1)]) #confluent Kafka-API helped here
    for f_var in futures.values():
        f_var.result()
        print(f"[setup] created topic '{Topic}' with {Partitions} partitions")
