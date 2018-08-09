"""
Produces a flow of wiki edit log raw data to a Kafka topic. The original data set has
historic timestamps, which are being replaced with current timestamps to simulate
a real-time process.

"""

import datetime
from datetime import timedelta
from kafka import KafkaProducer, KafkaConsumer
import time, sys
from time import gmtime, strftime

def main():
    producer = KafkaProducer(bootstrap_servers='ec2-52-10-89-2.us-west-2.compute.amazonaws.com')

    file_address = "/home/ubuntu/wikiedit560.txt"


    count = 0
    with open(file_address) as f:
        for line in f:
            curtime = strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            new_line = line + " " + curtime
            producer.send("wikiedittopic", new_line)
            count += 1
	    print("Now Kafka is sending... " + new_line)
    producer.flush()
    f.close()

if __name__ == "__main__":
    main()


