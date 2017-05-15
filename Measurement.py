import time
import os
import datetime
from hdfs.ext.dataframe import read_dataframe, write_dataframe
from hdfs import Config
import json
import pandas as pd
import pandasql as pdsql
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pymongo import MongoClient



def Reorganization():
	filenames = ['file0.csv','file1.csv','file2.csv','file3.csv','file4.csv','file5.csv','file6.csv','file7.csv','file8.csv','file9.csv']
	step = int(8760*3600/len(filenames))
	print step
	mongoClient = MongoClient('localhost',27017)
	db = mongoClient.test
	client = Config().get_client('dev')
	dataFilter = [None]*len(filenames)
	spark = SparkSession.builder.appName("Spark with secondary indexes").getOrCreate() 
	customSchema = StructType([
        StructField("_id", IntegerType(), True),
        StructField("meterid", IntegerType(), True),
        StructField("measurement", DoubleType(), True),
        StructField("date", TimestampType(), True),
        StructField("obs", StringType(), True)])

	sqlContext = SQLContext(spark)
	t1 = millis()
	df = sqlContext.read.format("org.apache.spark.csv").option("header", "false").schema(customSchema).csv("hdfs://localhost:54310/data-v2/*")
	df.createOrReplaceTempView("data")

	results = sqlContext.sql("SELECT row_number() over (order by measurement) as rank ,_id, meterid, measurement, date, obs FROM data").cache()


	minDate = 1356998400
	dir = '/data-re/'
	for i in range(0, len(filenames)):
		db.histogram.remove({"name": filenames[i]})
		dataFilter[i] = results.filter(results.rank >= i * 43800).filter(results.rank < 1 + (i + 1) * 43800).drop('rank')
		
		dataFilter[i].repartition(1).write.mode("append").csv('hdfs://localhost:54310/data-re/')
		
		pandasDF = dataFilter[i].toPandas()
		pandasDF.columns = ['id','meterid','measurement','date','comment']
		data = {}
		data["name"] = filenames[i]
		hist = []

		pysql = lambda q: pdsql.sqldf(q, {'pandasDF': pandasDF})
		sql1 = "select date, count(*) as count from pandasDF group by date order by date"

		df1 = pysql(sql1)
		size = len(df1.index)
	
		for num in range (size - 1, 0, -1):
			hist.append({"date": df1['date'][num], "cumulation": df1['count'][0:num+1].sum()})

		data["hist"] = hist
		json.dumps(data)
		db.histogram.insert_one(data)

		files = client.list(dir)
		for f in files:
			if "part-00" in f:
				client.rename(dir+f,dir+filenames[i])
		dataFilter[i].drop('obs').drop('date').withColumn('fname', lit(filenames[i])).write.format("com.mongodb.spark.sql.DefaultSource")\
               .option("spark.mongodb.output.uri", "mongodb://localhost:27017/test.population")\
               .mode("append")\
               .save()
	
	t2 = millis()
	print "\n"
	print "\n"
	print "\n"
	print str(t2 - t1)
	print "\n"
	print "\n"
	print "\n"

def millis():
    return int(round(time.time() * 1000))

if __name__ == "__main__":
    Reorganization()