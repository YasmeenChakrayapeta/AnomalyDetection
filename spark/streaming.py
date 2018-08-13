import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName=”PythonStreamingDirectKafka”)
ssc = StreamingContext(sc, 2)


kafka_topic = 'wikiedittopic'
kafka_broker = 'ip-10-0-0-7:9092'

postgres_url = 'jdbc:postgresql://ec2-52-43-225-145.us-west-2.compute.amazonaws.com:5434/postgresdb'
postgres_user = "postgres"
postgres_pw = "Insight123$"
postgres_driver = "org.postgresql.Driver"
postgres_properties = {'user': postgres_user, 'password':postgres_pw,'driver':postgres_driver}


kvs = KafkaUtils.createDirectStream(ssc, ['wikiedittopic'], {"bootstrap.servers": kafka_broker})
user_edits_stream = kvs.map(lambda x: x[1])
for i in user_edits_stream.take(10):print(i)

#df = sqlContext.createDataFrame(recordrdd4,['username','edit_timestamp','edit_count'])

#df.write.jdbc(url=postgres_url, table='flagged_users', mode='overwrite', properties=postgres_properties)

ssc.start()
ssc.awaitTermination()
