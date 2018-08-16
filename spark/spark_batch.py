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