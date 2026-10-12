
#detector : watches login events and flags brute-force attempts

#run up to four of these in separate terminals. Any extra will just be on standby
#    python detector.py --name det1
#    python detector.py --name det2
#    python detector.py --name det3
#    python detector.py --name det4
#kafka will splits the partitions between them. Stop one with Ctrl+C and Kafka hands its partitions to the survivor

import argparse
import json
# Recommended that you read the guide to understand confluent_kafka more
from confluent_kafka import Consumer

from common import Server, Topic, topic_check

Window = 60     # seconds to look back
Warn = 3     # soft warning
Alert = 5    # full alert


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    name = args.name

    topic_check()
    consumer = Consumer({
        "bootstrap.servers": Server,
        "group.id": "detectors",          
        "auto.offset.reset": "latest",    #only allows for the collection of data that now arrives
        "session.timeout.ms": 10000,      #10 seconds. Change if you want it to realize the a terminal is dead quicker.
    })

    def on_assign(c, partitions):
        print(f"[{name}] >>> assigned partitions: {[p.partition for p in partitions]}")

    def on_revoke(c, partitions):
        print(f"[{name}] <<< partitions revoked: {[p.partition for p in partitions]}")

    consumer.subscribe([Topic], on_assign=on_assign, on_revoke=on_revoke) # calls these functions if partitions are taken away or added. 

    failures = {}

    print(f"[{name}] detector started, waiting for events (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0) 
            if msg is None:
                continue
            if msg.error(): #needed to prevent crash
                print(f"[{name}] error: {msg.error()}")
                continue

            event = json.loads(msg.value()) #json is needed to turn the text into a dict that can be read. The sensor sends data as text
            if event["result"] != "fail":
                continue

            account = event["account"]
            now = event["ts"]
            times = failures.setdefault(account, [])
            times.append(now)
            times[:] = [t for t in times if now - t < Window]   # drops old failures after 60 secs
            count = len(times)

            print(f"[{name}] p{msg.partition()} failed login: {account} "
                  f"from {event['ip']} via {event['sensor']} ({count} in last {Window}s)")

            if count == Warn:
                print(f"[{name}] WARNING: {count} failed logins for '{account}' ")
            if count >= Alert:
                print(f"[{name}] ALERT: possible brute-force on '{account}' "
                      f"({count} failures in {Window}s)")
                times.clear()   # reset so one attack = one alert
    except KeyboardInterrupt:
        print(f"[{name}] shutting down")
    finally:
        consumer.close()  


if __name__ == "__main__":
    main()
