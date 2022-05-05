from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ast import literal_eval
import json
import hashlib

spark = SparkSession.builder.appName("Test").getOrCreate()

df = spark.read.csv('/home/david/Documents/projects/files',header=True,inferSchema=True)

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

df = df.select("record_uid", "timestamp", "match_url", "bookie", "odds")
df.show(20)