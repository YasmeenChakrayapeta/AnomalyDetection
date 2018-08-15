
"""
This file creates the two tables in Cassandra used for storage after stream processing.
To be able to execute, install cassandra-driver: sudo pip install cassandra-driver
"""

from cassandra.cluster import Cluster

CASSANDRA_RESOURCE_LOCATION = 'resources/cassandra.config'
CASSANDRA_KEYSPACE = 'ks'
#CASSANDRA_TABLE_VISITS_RANK = 'visit_rank'


# obtain cassandra hosts from config
with open(CASSANDRA_RESOURCE_LOCATION) as f:
	line1 = f.readline()
	cassandra_hosts = line1.strip().split('=')[1].split(',')
print("Obtained Cassandra hosts")
print(cassandra_hosts)

cluster = Cluster(cassandra_hosts)
sessionks = cluster.connect()
sessionks.execute("create keyspace if not exists ks  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };")

session = cluster.connect(CASSANDRA_KEYSPACE)

session.execute("CREATE TABLE if not exists totalInputCountSecond (global_id text, edit_time text, count bigint, PRIMARY KEY (global_id, edit_time)) with clustering order by (edit_time desc);")
session.execute("CREATE TABLE if not exists UserCountsingle (username text, edit_time text, count bigint, PRIMARY KEY (username, edit_time)) with clustering order by (edit_time desc);")
session.execute("CREATE TABLE if not exists  Usersflagged (username text, edit_time text, count bigint, PRIMARY KEY (username, edit_time)) with clustering order by (edit_time desc);")
session.execute("CREATE TABLE if not exists UseravgActivity (username text, count bigint, PRIMARY KEY (username));")
