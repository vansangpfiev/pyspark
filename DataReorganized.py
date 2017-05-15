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
from Constant import Constant


class DataReorganized:
	def __init__(self, host, port):
		self.mongoClient = MongoClient(host, port)
		self.db = self.mongoClient.indexes
		self.hdfsClient = Config().get_client('dev')
		self.spark = SparkSession.builder.appName("Spark with secondary indexes").getOrCreate() 
		self.customSchema = StructType([
        	StructField("_id", IntegerType(), True),
        	StructField("meterid", IntegerType(), True),
        	StructField("measurement", DoubleType(), True),
        	StructField("date", TimestampType(), True),
        	StructField("obs", StringType(), True)])
		self.sqlContext = SQLContext(self.spark)


	def reorganizeByTimestamp(self, filenames, hdfsClientDir):
		# filenames = ['file0.csv','file1.csv','file2.csv','file3.csv','file4.csv','file5.csv','file6.csv','file7.csv','file8.csv','file9.csv']
		HOURS_PER_FILE = 8760 * 3600
		MIN_TIMESTAMP = 1356998400   #2013-01-01 00:00:00
		TIMESTAMPS_PER_FILE = int(HOURS_PER_FILE/len(filenames))
		hdfsClient = self.hdfsClient
		sqlContext = self.sqlContext
		customSchema = self.customSchema
	
		hdfsFiles = ""
		filesToDelete = filenames
		
		for f in filenames:
			hdfsFiles += Constant.HDFS_DIR + f + ","
		
		dataAfterReorganized = [None]*len(filenames)

		df = sqlContext.read.format("org.apache.spark.csv").option("header", "false").schema(customSchema).csv(hdfsFiles[:len(hdfsFiles) - 1].split(','))
		df.createOrReplaceTempView("data")

		dataOrderByDate = sqlContext.sql("SELECT _id, meterid, measurement, date, obs FROM data ORDER BY date").cache()
		
		filenames = appendTimestampPrefix(filenames)	
		for i in range(0, len(filenames)):
			self.db.histogram.remove({"name": filesToDelete[i]})
			self.db.meterdata.remove({"fname": filesToDelete[i]})

			dataAfterReorganized[i] = dataOrderByDate.filter(dataOrderByDate.date >= datetime.datetime.fromtimestamp(MIN_TIMESTAMP + TIMESTAMPS_PER_FILE*i)).filter(dataOrderByDate.date < datetime.datetime.fromtimestamp(MIN_TIMESTAMP + TIMESTAMPS_PER_FILE*(i + 1)))
			dataAfterReorganized[i].repartition(1).write.mode("append").csv(Constant.HDFS_DIR)
		
			self.createHistogramByPandas(filenames[i], dataAfterReorganized[i])

			files = hdfsClient.list(hdfsClientDir)
			for f in files:
				if "part-00" in f:
					hdfsClient.rename(hdfsClientDir+f,hdfsClientDir+filenames[i])

			dataAfterReorganized[i].drop('obs').drop('date').withColumn('fname', lit(filenames[i])).write.format("com.mongodb.spark.sql.DefaultSource")\
               .option("spark.mongodb.output.uri", Constant.MONGO_METERDATA_COLLECTION_DIR)\
               .mode("append")\
               .save()
		self.deleteHDFSFiles(filesToDelete, hdfsClientDir)
		


	def reorganizeByMeasurement(self, filenames, hdfsClientDir):
		hdfsClient = self.hdfsClient
		sqlContext = self.sqlContext
		customSchema = self.customSchema
		hdfsFiles = ""
		for f in filenames:
			hdfsFiles += Constant.HDFS_DIR + f + ","
		dataAfterReorganized = [None]*len(filenames)
		
		filesToDelete = filenames
		
		filenames = appendMeasurementPrefix(filenames)

		df = sqlContext.read.format("org.apache.spark.csv").option("header", "false").schema(customSchema).csv(hdfsFiles[:len(hdfsFiles) - 1].split(','))
		df.createOrReplaceTempView("data")

		dataOrderByMeasurement = sqlContext.sql("SELECT row_number() over (order by measurement) as rank ,_id, meterid, measurement, date, obs FROM data").cache()
		for i in range(0, len(filenames)):
			self.db.histogram.remove({"name": filesToDelete[i]})
			self.db.meterdata.remove({"fname": filesToDelete[i]})
			dataAfterReorganized[i] = dataOrderByMeasurement.filter(dataOrderByMeasurement.rank >= i * 43800).filter(dataOrderByMeasurement.rank < 1 + (i + 1) * 43800).drop('rank')
		
			dataAfterReorganized[i].repartition(1).write.mode("append").csv(Constant.HDFS_DIR)
		
			self.createHistogramByPandas(filenames[i], dataAfterReorganized[i])

			files = hdfsClient.list(hdfsClientDir)
			for f in files:
				if "part-00" in f:
					hdfsClient.rename(hdfsClientDir+f,hdfsClientDir+filenames[i])
			dataAfterReorganized[i].drop('obs').drop('date').withColumn('fname', lit(filenames[i])).write.format("com.mongodb.spark.sql.DefaultSource")\
               .option("spark.mongodb.output.uri", Constant.MONGO_METERDATA_COLLECTION_DIR)\
               .mode("append")\
               .save()
		self.deleteHDFSFiles(filesToDelete, hdfsClientDir)
		# dir = '/data-re/'

	def reorganizeByMeterId(self, filenames, hdfsClientDir):
		
		hdfsClient = self.hdfsClient
		hdfsFiles = ""
		for f in filenames:
			hdfsFiles += Constant.HDFS_DIR + f + ","
		dataAfterReorganized = [None]*len(filenames)
		sqlContext = self.sqlContext
		customSchema = self.customSchema
		filesToDelete = filenames
		
		filenames = appendMeterPrefix(filenames)

		df = sqlContext.read.format("org.apache.spark.csv").option("header", "false").schema(customSchema).csv(hdfsFiles[:len(hdfsFiles) - 1].split(','))
		df.createOrReplaceTempView("data")

		dataOrderByMeterId = sqlContext.sql("SELECT row_number() over (order by meterid) as rank ,_id, meterid, measurement, date, obs FROM data").cache()

		for i in range(0, len(filenames)):
			self.db.histogram.remove({"name": filesToDelete[i]})
			self.db.population.remove({"fname": filesToDelete[i]})
			dataAfterReorganized[i] = dataOrderByMeterId.filter(dataOrderByMeterId.rank >= i * 43800).filter(dataOrderByMeterId.rank < 1 + (i + 1) * 43800).drop('rank')
		
			dataAfterReorganized[i].repartition(1).write.mode("append").csv(Constant.HDFS_DIR)
		
			self.createHistogramByPandas(filenames[i], dataAfterReorganized[i])

			files = hdfsClient.list(hdfsClientDir)
			for f in files:
				if "part-00" in f:
					hdfsClient.rename(hdfsClientDir+f,hdfsClientDir+filenames[i])
			dataAfterReorganized[i].drop('obs').drop('date').withColumn('fname', lit(filenames[i])).write.format("com.mongodb.spark.sql.DefaultSource")\
               .option("spark.mongodb.output.uri", Constant.MONGO_METERDATA_COLLECTION_DIR)\
               .mode("append")\
               .save()
		self.deleteHDFSFiles(filesToDelete, hdfsClientDir)
		# dir = '/data-re/'
		

	def createHistogramByPandas(self, filename, sparkDataFrame):
		pandasDF = sparkDataFrame.toPandas()
		pandasDF.columns = ['id','meterid','measurement','date','comment']
		data = {}
		data["name"] = filename
		hist = []

		pysql = lambda q: pdsql.sqldf(q, {'pandasDF': pandasDF})
		sql1 = "select date, count(*) as count from pandasDF group by date order by date"

		df1 = pysql(sql1)
		print df1
		size = len(df1.index)
	
		for num in range (size - 1, 0, -1):
			hist.append({"date": (df1['date'][num]).split('.')[0], "cumulation": df1['count'][0:num+1].sum()})

		data["hist"] = hist
		json.dumps(data)
		self.db.histogram.insert_one(data)	

	def deleteHDFSFiles(self, filenames, hdfsClientDir):
		hdfsClient = self.hdfsClient
		allHDFSFiles = hdfsClient.list(hdfsClientDir)
		for f in allHDFSFiles:
			if f in str(filenames):
				hdfsClient.delete(hdfsClientDir+f)

def millis():
    return int(round(time.time() * 1000))

def appendMeasurementPrefix(filenames):
	for i in range(0, len(filenames) - 1):
		filenames[i] = filenames[i].replace('time-', '')
	return ['mea-' + s for s in filenames]

def appendTimestampPrefix(filenames):
	for i in range(0, len(filenames) - 1):
		filenames[i] = filenames[i].replace('mea-', '')
	return ['time-' + s for s in filenames]

def appendMeterPrefix(filenames):
	for i in range(0, len(filenames) - 1):
		filenames[i] = filenames[i].replace('time-', '')
		filenames[i] = filenames[i].replace('mea-', '')
	return filenames

if __name__ == "__main__":
    Reorganization()