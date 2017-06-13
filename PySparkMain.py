import time
import os

import Producer
from Producer import Producer
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *
import Metric
from Metric import Metric
import MongoIndex
from MongoIndex import MongoIndex
from FilterExpression import OperatorType
from queries import *

def main(qtype, qfile, filters, filters1, mnode):

    querytype = qtype
    tbName = "indexes"
    if (querytype == 'query1') :
        querystring = queries(tbName).query1
    elif (querytype == 'query2') :
        querystring = queries(tbName).query2
    elif (querytype == 'query3') :
        querystring = queries(tbName).query3
    elif (querytype == 'query4') :
        querystring = queries(tbName).query4
    elif (querytype == 'query5') :
        querystring = queries(tbName).query5
    elif (querytype == 'query6') :
        querystring = queries(tbName).query6
    elif (querytype == 'query7') :
        querystring = queries(tbName).query7
    else :
        print "query " + querytype + "unknown"

    ############### index scan
     
    mongoIndex = MongoIndex(mnode, 27017)
    #filters = [FilterExpression("measurement", "0.2", OperatorType.GREATER), FilterExpression("clientid", "20", OperatorType.LOWER)]
#tdir = qfile.split('/')
#   tdir = tdir[len(tdir)-1]
#   ddir  = qfile.replace('/'+tdir, '')
#   ddir  = ddir.replace('dfs.`', '')
#   ddir  = ddir.replace('hdfs.`', '')
    ddir = qfile
#ddir = "/users/chihoub/workspace/smartmeterdata/data-v2/meterdata"
    t1 = millis()
    names = mongoIndex.getFileNames(ddir, filters)
    t2 = millis()
    t_index = t2 - t1
    # print names
    #names = ddir + '/file0.csv'
    

    metric = Metric(mnode, 27017)
    factors = metric.getMetricFactors(querytype, names, filters, filters1)
    print factors
    
    pro = Producer(mnode, 9092)
    pro.publicMessage(str(factors))

    print 'dgdfgdfdfdf'
    print
    print
    print
    print querystring

    print t_index

    spark = SparkSession.builder \
        .appName("Spark with secondary indexes") \
        .getOrCreate() \
        # .enableHiveSupport() \

    sqlContext = SQLContext(spark)

    customSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("meterid", IntegerType(), True),
        StructField("measurement", DoubleType(), True),
        StructField("date", TimestampType(), True),
        StructField("obs", StringType(), True)])

    #names = 'hdfs://parasilo-1:8020/user/meterdata/file0.csv,hdfs://parasilo-1:8020/user/meterdata/file1.csv'
    df = sqlContext.read \
        .format("org.apache.spark.csv") \
        .option("header", "false") \
        .schema(customSchema) \
        .csv(names.split(','))

    df.createOrReplaceTempView(tbName)
#df.registerTempTable(tbName)
#s = 'select * from ' + tbName

    t1 = millis()
    results = sqlContext.sql(querystring)
    results.show()
    t2 = millis()
    t_query = t2 - t1



    log_fname = os.getcwd() + "/sexecTime"
    with open(log_fname, "a") as myfile:
        myfile.write(qtype + "," + str(t_index) + "," + str(t_query) + "," + str(t_index + t_query) + "\n")
    
#print qtype + "," + str(t_index) + "," + str(t_plan) + "," + str(t_query) + "," + str(t_index+t_plan+t_query)
    


def millis():
    return int(round(time.time() * 1000))



if __name__ == "__main__":
    main()


