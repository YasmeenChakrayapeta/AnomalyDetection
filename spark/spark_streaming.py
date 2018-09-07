from __future__ import print_function

from pyspark.sql.window import Window
import pyspark.sql.functions as func

import sys
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
# cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from datetime import datetime

# encoding=utf8
#import sys
reload(sys)
sys.setdefaultencoding('utf8')

"""
 spark-submit --master spark://ip-10-0-0-12:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark-streaming.py
"""
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'

CASSANDRA_RESOURCE_LOCATION = 'cassandra.config'
#KAFKA_RESOURCE_LOCATION = 'resources/kafka.config'

CASSANDRA_KEYSPACE = 'ks'
#CASSANDRA_TABLE = 'real_time'

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


def sendCassandra1(iter):
    print("send to cassandra")
    cluster = Cluster(cassandra_hosts)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.set_keyspace("ks")

    insert_statement1 = session.prepare("INSERT INTO totalInputCountSecond (global_id,edit_time,count) VALUES (?,?,?)")

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    count=0
    for record in iter.collect():
        batch.add(insert_statement1, ('a',record[0], record[1]))
        count += 1
        if count % 500 == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    session.execute(batch)
    session.shutdown()

def sendCassandra2(iter):
    print("send to cassandra")
    cluster = Cluster(cassandra_hosts)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.set_keyspace("ks")

    insert_statement2 = session.prepare("INSERT INTO usercountsingle (username,edit_time,count) VALUES (?,?,?)")

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    count=0
    for record in iter.collect():
	batch.add(insert_statement2, (record[0][0], record[0][1],record[1]))
	count += 1
        if count % 500 == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    session.execute(batch)
    session.shutdown()

def sendCassandra3(iter):
    print("send to cassandra")
    cluster = Cluster(cassandra_hosts)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.set_keyspace("ks")

    insert_statement3 = session.prepare("INSERT INTO usersflagged (username,edit_time,count) VALUES (?,?,?)")

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    count=0
    for record in iter.collect():
        batch.add(insert_statement3, (record[0][0],record[0][1], record[1]))
        count += 1
        if count % 500 == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    session.execute(batch)
    session.shutdown()

def sendCassandra4(iter):
    print("send to cassandra")
    cluster = Cluster(cassandra_hosts)
    session = cluster.connect(CASSANDRA_KEYSPACE)
    session.set_keyspace("ks")

    insert_statement4 = session.prepare("INSERT INTO useravgactivity (username,count) VALUES (?,?)")

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    count=0
    for record in iter.collect():
        batch.add(insert_statement4, (record[0], record[1]))
        count += 1
        if count % 500 == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    session.execute(batch)
    session.shutdown()

# obtain data stream from the kafka topic
print("test before")
kafkaStream = KafkaUtils.createDirectStream(ssc, ['wikiedittopic'], {"bootstrap.servers": kafka_broker})
#print(kafkaStream)
print("test after")

# aggregations (by sec)

user_edits = kafkaStream.map(lambda x: x[1])
print('user_edits')
user_edits.pprint(10)

user_edits_lines = user_edits.map(lambda line: line.split(' '))
print('user_edits_lines')
user_edits_lines.pprint(10)

totalInput = user_edits_lines.map(lambda x: (x[7] +" "+x[8]))
print('totalinput')
totalInput.pprint(10)

totalInputtime = totalInput.map(lambda x: (x,1))
print('totalinputtime')
totalInputtime.pprint(10)
totalInputtimetotal = totalInputtime.reduceByKey(lambda a, b: a + b)
print('totalinputtimetotal')
totalInputtimetotal.pprint(10)


#singleUserCountSecond
singleUserCountSecond = user_edits_lines.map(lambda x: (x[5],(x[7] +" "+x[8])))
totaltimesingleuser = singleUserCountSecond.map(lambda x: (x,1))
"""
windowDuration = '{} seconds'.format(1)dd
slideDuration = '{} seconds'.format(1)
totaltimesinglewindow = totaltimesingleuser.groupBy(window(totaltimesingleuser[0], windowDuration, slideDuration),totaltimesingleuser[1]).orderBy('window')
"""
totalsecondsingleuserrdd= totaltimesingleuser.reduceByKey(lambda a, b: a + b)
totalsecondsingleuserrdd.pprint(10)


#usersflagged
usersflagged = user_edits_lines.map(lambda x: (x[5], x[7] +" "+x[8]))
usersflaggedcnt = usersflagged.map(lambda x: (x,1))
#usersflaggedmorecnt = usersflaggedcnt.map(lambda x: (x,1)).filter()
usersflaggedcntrdd= usersflaggedcnt.reduceByKey(lambda a, b: a + b)

#usersavgactivity
usersavgactivity = user_edits_lines.map(lambda x: (x[5]))
usersavgactivitycnt = usersavgactivity.map(lambda x: (x,1))
#usersflaggedmorecnt = usersflaggedcnt.map(lambda x: (x,1)).filter()
usersavgactivitycntrdd= usersavgactivitycnt.reduceByKey(lambda a, b: a + b)


#Loading data into Cassandra

totalInputtimetotal.foreachRDD(sendCassandra1)
totalsecondsingleuserrdd.foreachRDD(sendCassandra2)
usersflaggedcntrdd.foreachRDD(sendCassandra3)
usersavgactivitycntrdd.foreachRDD(sendCassandra4)


ssc.start()
ssc.awaitTermination()
