from pyspark.sql import DataFrameWriter
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


# Set Spark context
sc = SparkContext("local","datafile")
# Set Spark SQL context
sqlContext = SQLContext(sc)
# Read wiki edit log data from S3
datafile = "s3a://wikieditlog-anamoly/sourcefile/data/wikieditfile6.txt"
wikifile = sc.textFile(datafile)
#print('getting the FIRST and SECOND columns START')
#record = wikifile.map(lambda x: x.split(" "))
#record = wikifile.map(lambda x: (x.split(" ")[0], x.split(" ")[1], x.split(" ")[2], x.split(" ")[3], x.split(" ")[4].replace('T',' ').replace('Z',''),x.split(" ")[5],x.split(" ")[6] ))
#user_edits_lines = record.flatMap(lambda line: (line.split(" ")[5], line.split(" ")[4].replace('T',' ').replace('Z',''))).map(lambda x:(x,1)).reduceByKey(lambda a, b: a + b)
#user_edits_lines = record.map(lambda line: ((line.split(" ")[5], line.split(" ")[4].replace('T',' ').replace('Z','')),1)).reduceByKey(lambda a, b: a + b)
#user_edits_lines = record.map(lambda line: ((line.split(" ")[4], line.split(" ")[5]),1)).reduceByKey(lambda a, b: a + b)
recordrdd1 = wikifile.map(lambda x: ( x.split(" ")[5], x.split(" ")[4].replace('T',' ').replace('Z','')))
recordrdd2 = recordrdd1.map(lambda x: (x,1))
recordrdd3 = recordrdd2.reduceByKey(lambda a, b: a + b)
recordrdd4 = recordrdd3.map(lambda x: (x[0][0],x[0][1],x[1]))
#recordrdd4 = recordrdd3.filter(lambda x: x[1] >100)
#user_edits_lines = record.flatMap(lambda line: line.split(" ")).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
"""
for i in recordrdd1.take(10):print(i)
print("group by rdd")
for i in recordrdd2.take(10):print(i)
print("group by rdd3")
for i in recordrdd3.take(10):print(i)
print("rdd4")

for i in recordrdd4.take(10):print(i)
"""
#print('getting the FIRST and SECOND columns DONE')
#edits = wikifile.map(lambda x: x.split(" ")[0],x.split(" ")[1])
#for i in edits.take(10):print(i)

"""
url_connect = "jdbc:postgresql://52.43.225.145:5432/postgresdb"
table = "real_time"
mode = "overwrite"
properties = {"user": "postgres", "password": "Insight123$", "driver": "org.postgresql.Driver"}
"""
postgres_url = 'jdbc:postgresql://ec2-52-43-225-145.us-west-2.compute.amazonaws.com:5434/postgresdb'
postgres_user = "postgres"
postgres_pw = "Insight123$"
postgres_driver = "org.postgresql.Driver"
postgres_properties = {'user': postgres_user, 'password':postgres_pw,'driver':postgres_driver}

df = sqlContext.createDataFrame(recordrdd4,['username','edit_timestamp','edit_count'])

df.write.jdbc(url=postgres_url, table='flagged_users', mode='append', properties=postgres_properties)


