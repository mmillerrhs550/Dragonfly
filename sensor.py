
#sensor: pretends to be a server writing login logs.
#normal:  python sensor.py --name ssh
#attack:  python sensor.py --name vpn --attack admin --count 5
import argparse
import json
import random
import time

from confluent_kafka import Producer

from common import Server, Topic, topic_check

users = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi", "ivan", "judy", "mallory", "oscar", "john", "wyatt", "richard", "michael", "olivia", "randy"] #these will be split up in the different partitions
Attacker_IP = "203.0.113.66"


def random_ip():
    return f"10.0.{random.randint(1,255)}.{random.randint(1, 254)}" 


def make_event(sensor, account, result, ip):
    return {"sensor": sensor, "account": account, "result": result,
            "ip": ip, "ts": time.time()}


def send(producer, event):
    producer.produce(Topic, key=event["account"], value=json.dumps(event)) #turns dict into text string for detector to pick up
    producer.poll(0)  


def main():
    parser = argparse.ArgumentParser(description="Fake server that sends login events to Kafka") #allows for command prompt input to affect variables
    parser.add_argument("--name", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--fail-rate", type=float, default=0.05)
    parser.add_argument("--attack", metavar="ACCOUNT")
    parser.add_argument("--count", type=int, default=5)
    args = parser.parse_args()

    topic_check() 
    producer = Producer({"bootstrap.servers": Server}) 

    if args.attack:
        print(f"[{args.name}] launching brute-force attack on '{args.attack}' from {Attacker_IP}")
        for i in range(args.count):
            send(producer, make_event(args.name, args.attack, "fail", Attacker_IP))
            print(f"[{args.name}] Attack{i + 1}/{args.count}: failed login for '{args.attack}'")
            time.sleep(0.5)
        producer.flush() 
        return

    print(f"[{args.name}] sensor started, one event every {args.interval}s (Ctrl+C to stop)")
    try:
        while True:
            account = random.choice(users)
            result = "fail" if random.random() < args.fail_rate else "success"
            event = make_event(args.name, account, result, random_ip())
            send(producer, event)
            print(f"[{args.name}] {result:7} login for {account} from {event['ip']}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"[{args.name}] stopping")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
