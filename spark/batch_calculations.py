# import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
CASSANDRA_KEYSPACE = 'wikieditloganalytics'
CASSANDRA_TABLE_EDIT_LOG = 'edit_log'

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
datafile = "s3a://wikieditlog-anamoly/sourcefile/raw/data/wikiedit560.txt"
wikirawdatardd = sc.textFile(datafile)
for i in wikirawdatardd.take(10):print(i)

# Split lines into columns by delimiter '\t'
record = wikirawdatardd.map(lambda x: x.split(" "))
#record = wikirawdatardd.map(lambda x: x.split(",")[2])
print(record)
#filterdatardd = record.filter(lambda x: len(x[1])>=2)
#filteredrdd=record.filter(lambda r: r[2]==134344823)
for i in record.take(10):print(i)

print('printing record')
#print(record[1],record[2])
#print(record)

# Convert Rdd into DataFrame
df_edit_log = sqlContext.createDataFrame(record,['article_id', 'rev_id', 'article_title', 'edittimestamp', 'username', 'user_id'])
#df_edit_log = sqlContext.createDataFrame(filteredrdd,['article_id', 'rev_id', 'article_title', 'edittimestamp', 'username', 'user_id'])
for i in df_edit_log.take(10):print(i)
print('printing df_edit_log')

df_edit_log.write.format("org.apache.spark.sql.cassandra").mode('append').options(table='edit_log', keyspace=CASSANDRA_KEYSPACE).save()

#datavalidation = session.execute('select * from visits_rank ;')
#print(datavalidation)
