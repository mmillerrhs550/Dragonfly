# Dragonfly: Distributed Brute-Force Login Detector

A small distributed security monitor built on Apache Kafka and Python
Several "sensor" nodes pretend to be servers (web, SSH, VPN, adn etc...) and stream login
events into Kafka. Several "detector" nodes share the work of reading those
events and raise an alert when one account sees too many failed logins in a
short time. This simulates a distribute network by allowing for the partitioning of the work. 
If one detector node goes down, its load will be split among the remaining detector nodes.

## File Composition

- common.py: Holds the shared setting needed for the other python scripts.
This script creates the topic needed for the login detector to run, and then customizes it for the project. 
A built-in check is embedded to ensure that topic is generated as well.

- sensor.py: This script will simulate a server and generate login attempts. There is built-in customization that allows for features such as the name and login failure rates to be adjusted.
One features is the allowance of a spam mode which creates rapid failed attempts to simulate an attempted login break-in.
Each login in attempt will generate an IP and a name. The names will divided by the four partitions to be watched by detectors.

- detector.py: this script will create recievers that Kafka will allocate dynamically partitions to. Each partition will be allocated certain login names. 
Then regardless of what sensor, the partition will will receive information.
If the detector detects three failed attempts on an account in a minute, it will place out a warning in the terminal. 
If it detects five, it will submit an alert in the terminal.

- README.md: this file which will explain this detector in detail.

- docker-compose.yml: This is the file that starts Kafka in a container. This allows for other devices 
to simulate this detector without setting up Java and Kafka on their local devices.

## Requirements

- Linux (tested on Linux Mint), macOS or Windows with Docker (Windows is unverified)
- Docker with the Compose plugin (`sudo apt install docker.io docker-compose-v2` may be needed)
- Python 3.1+
- Confluent-Kafka installed. A docker mgiht be more convenient. The Confluent-Kafka website has a guide for this.

## How to run (on Linux Mint Terminal)

1. Start Kafka (from the project folder):


`sudo docker compose up -d`


2. Set up Python (a virtual environment is needed):

`sudo apt install python3-venv`
`python3 -m venv venv` 
`source venv/bin/activate`
`pip install confluent-kafka`

Run `source venv/bin/activate` in every new terminal before the steps below. Also ensure you are in the project folder for each new window

3. Start five detectors, each in its own terminal:

`python detector.py --name det1`
`python detector.py --name det2`
`python detector.py --name det3`
`python detector.py --name det4`
`python detector.py --name det5`

4. Start three sensors, each in its own terminal:

`python sensor.py --name web`
`python sensor.py --name ssh` (to toggle fail-rate utilize `--fail-rate 0.20`.)
`python sensor.py --name vpn`


5. Launch an attack in another terminal (Optional):

`python sensor.py --name vpn --attack mallory --count 5`

6. Show fault tolerance. Press `Ctrl+C` in the detector that caught the
attack. The other detector prints `>>> assigned partitions` with the extra
partitions. Run the attack again and the surviving detector catches it.

7. Shut down:`Ctrl+C` in each terminal, then `sudo docker compose down`.

