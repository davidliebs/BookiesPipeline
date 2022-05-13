from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType

import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')

from ast import literal_eval
import json
import hashlib
import mysql.connector

spark = SparkSession.builder.appName("Test").getOrCreate()

# reading in csv's
fileSchema = StructType().add("Unnamed: 0", "integer").add("match_url", "string").add("bookie", "string").add("odds", "string")
df = spark.readStream.option('sep', ',').option('header', 'true').schema(fileSchema).csv('/home/david/Documents/projects/files')


# applying transformations
def CreatePrimaryKey(match_url, bookie):
	primary_key_string = match_url + bookie
	primary_key_string = primary_key_string.encode()

	primary_key_hash = hashlib.md5(primary_key_string).hexdigest()

	return primary_key_hash

createPrimaryKeyUDF = F.udf(lambda x, y: CreatePrimaryKey(x, y), StringType())

def ConvertProbabilityToDecimal(probability_list):
	decimal_list = []
	probability_list = literal_eval(probability_list)
	for i in probability_list:
		num,den = i.split("/")
		decimal_odd = 1 + float(num) / float(den)
		
		decimal_list.append(decimal_odd)
		
	return decimal_list

convertOddsUDF = F.udf(lambda z: ConvertProbabilityToDecimal(z), StringType())

df = df.withColumn("record_uid", createPrimaryKeyUDF(df.match_url, df.bookie))
df = df.withColumn("decimal_odds", convertOddsUDF(df.odds))
df = df.withColumn("timestamp", F.current_timestamp())
df = df.withColumn("file_name", F.input_file_name())
df = df.select("record_uid", "timestamp", "match_url", "bookie", "decimal_odds", "file_name")

# dumping to mysql
def process(df, epoch_id):
	conn = mysql.connector.connect(
		user="david",
		password="open1010",
		host="127.0.0.1",
		port=3306,
		database="BookiesPipelineDB"
	)
	cur = conn.cursor()

	for row in df.collect():
		cur.execute("""
		
		INSERT INTO bookie_odds (record_uid, timestamp, match_url, bookie, odds, source_filename)
		VALUES('{}', '{}', '{}', '{}', '{}', '{}') 
		ON DUPLICATE KEY UPDATE timestamp='{}', odds='{}', source_filename='{}'
		""".format(row[0], row[1], row[2], row[3], row[4], row[5], row[1], row[4], row[5]))

		conn.commit()
	
	conn.close()

query = df.writeStream.foreachBatch(process).start()
query.awaitTermination()