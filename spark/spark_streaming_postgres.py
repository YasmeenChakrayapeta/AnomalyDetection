from __future__ import print_function

import sys
from pyspark.sql import DataFrameWriter
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


"""
 spark-submit --master spark://ip-10-0-0-12:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark-streaming.py
"""
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'


kafka_topic = 'wikiedittopic'
kafka_broker = 'ip-10-0-0-7:9092'

postgres_url = 'jdbc:postgresql://ec2-52-43-225-145.us-west-2.compute.amazonaws.com:5434/postgresdb'
postgres_user = "postgres"
postgres_pw = "Insight123$"
postgres_driver = "org.postgresql.Driver"
postgres_properties = {'user': postgres_user, 'password':postgres_pw,'driver':postgres_driver}

"""
registering the spark context
"""
#conf = SparkConf().setAppName("listen_stream")
#sc = SparkContext(conf=conf)
sc=SparkContext()
print("printing sc")
print(sc)
"""
registering the streaming context
"""
#ssc = StreamingContext(sc, 5)
ssc = StreamingContext(sc,5)

print( "prinitng after ssc")
# obtain data stream from the kafka topic

kafkaStream = KafkaUtils.createDirectStream(ssc, ['wikiedittopic'], {"bootstrap.servers": kafka_broker})


# aggregations (by timestamp)

user_edits_stream = kafkaStream.map(lambda x: x[1])
#user_edits_split = user_edits_stream.map(lambda lines: lines.split('')[0])
recordrdd1 = user_edits_stream.map(lambda x: ( x.split(" ")[5], x.split(" ")[7].replace('T',' ').replace('Z','')))
recordrdd2 = recordrdd1.map(lambda x: (x,1))
recordrdd3 = recordrdd2.reduceByKey(lambda a, b: a + b)
recordrdd4 = recordrdd3.map(lambda x: (x[0][0],x[0][1],x[1]))


df = sqlContext.createDataFrame(recordrdd4,['username','edit_timestamp','edit_count'])

df.write.jdbc(url=postgres_url, table='flagged_users', mode='overwrite', properties=postgres_properties)


ssc.start()
ssc.awaitTermination()

