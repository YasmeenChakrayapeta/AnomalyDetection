import os
import datetime
import time, sys
from time import gmtime, strftime

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel



CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
CASSANDRA_KEYSPACE = 'batchks'
CASSANDRA_TABLE_EDIT_LOG = 'edit_log_total_input'

# obtain cassandra hosts from config
with open(CASSANDRA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	cassandra_hosts = line1.strip().split('=')[1].split(',')

cluster = Cluster(cassandra_hosts)
session = cluster.connect(CASSANDRA_KEYSPACE)
print("Obtained Cassandra hosts")

# Set Spark context
sc = SparkContext("local","datafile")
# Set Spark SQL context
sqlContext = SQLContext(sc)
# Read wiki edit log data from S3
datafile = "s3a://wikieditlog-anamoly/sourcefile/data/wikieditfile1.txt"
wikirawdatardd = sc.textFile(datafile)
for i in wikirawdatardd.take(10):print(i)

# Split lines into columns by delimiter '\t'
record = wikirawdatardd.map(lambda x: x.split(" "))
for i in record.take(10):print(i)
filterdatardd = record.filter(lambda x: x[5][:2] <> "ip")
#filterdatardd = record.filter(lambda x: x[5] not in ["ip:office.bomis.com","ip:cobrand.bomis.com"])
for i in filterdatardd.take(10):print(i)

#filteredrdd=record.filter(lambda r: r[2]==134344823)

# Convert Rdd into DataFrame
df_edit_log = sqlContext.createDataFrame(filterdatardd,['revision','article_id', 'rev_id', 'article_title', 'edittimestamp', 'username', 'user_id'])

df_edit_log.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='edit_log_total_input', keyspace=CASSANDRA_KEYSPACE).save()

#datavalidation = session.execute('select * from visits_rank ;')
#print(datavalidation)

ubuntu@ip-10-0-0-4:~$
ubuntu@ip-10-0-0-4:~$ cat spark_streaming
cat: spark_streaming: No such file or directory
ubuntu@ip-10-0-0-4:~$ cat spark_streaming.py

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

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
#KAFKA_RESOURCE_LOCATION = 'resources/kafka.config'

CASSANDRA_KEYSPACE = 'ks'
#CASSANDRA_TABLE = 'real_time'

kafka_topic = 'wikiedittopic'
kafka_broker = 'ip-10-0-0-7:9092'

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
