from __future__ import print_function

import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
# cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from datetime import datetime

"""
 spark-submit --master spark://ip-10-0-0-12:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark-streaming.py
"""
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
# KAFKA_RESOURCE_LOCATION = 'resources/kafka.config'

CASSANDRA_KEYSPACE = 'wikieditloganalytics'
CASSANDRA_TABLE = 'real_time'

kafka_topic = 'wikiedittopic'
kafka_broker = 'ip-10-0-0-11:9092'

"""
obtain kafka brokers from config


with open(KAFKA_RESOURCE_LOCATION) as f:
        kafka_topic = f.readline().strip()
        kafka_broker = f.readline().strip()
        print ('kakfa topic: '  ,kafka_topic)
        print ('kafka broker: ' ,kafka_broker)
"""

"""
obtain cassandra hosts from config
"""

with open(CASSANDRA_RESOURCE_LOCATION) as f:
    line1 = f.readline()
    cassandra_hosts = line1.strip().split('=')[1].split(',')

"""
registering the spark context
"""
conf = SparkConf().setAppName("listen_stream")
sc = SparkContext(conf=conf)

"""
registering the streaming context
"""
ssc = StreamingContext(sc, 5)


# Need to work on the checkpoint


def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(cassandra_hosts)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.set_keyspace("wikieditloganalytics")

    insert_statement = session.prepare("INSERT INTO real_time (article_id,rev_id) VALUES (?,?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for record in iter.collect():
        batch.add(insert_statement, (int(record[1]), int(record[2])))

        print("Printing Kafka Stream")
        print(record[1] + "record1+record2" + record[2])
    session.execute(batch)
    session.shutdown()


# initial state RDD
# initialstateRDD = sc.parallelize([])

# obtain data stream from the kafka topic

kafkaStream = KafkaUtils.createDirectStream(ssc, ['wikiedittopic'], {"bootstrap.servers": kafka_broker})
# kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic], {"metadata.broker.list": kafka_brokers}, valueDecoder=avro_decoder)


# aggregations (by sec)

user_edits = kafkaStream.map(lambda x: x[1])
user_edits_lines = user_edits.map(lambda line: line.split(' '))
# processed_user_edit = user_edits_lines.map(lambda x: (x[5]+'_'+x[7]+' '+x[8]),1).reduceByKey(lambda a, b : a + b)
# processed_user_edit_counts = processed_user_edit.map(lambda (x): {'user':x[0].split["_"][0], 'editdatetime':x[0].split["_"][1],'count':str(x[1])})

user_edits_lines.pprint(10)

# processed_user_edit_counts.foreachRDD(sendCassandra)
user_edits_lines.foreachRDD(sendCassandra)
ssc.start()
ssc.awaitTermination()