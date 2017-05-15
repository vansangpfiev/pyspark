import sys
import ast
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from DataReorganized import DataReorganized


def millis():
    return int(round(time.time() * 1000))


def main():	
	WINDOW_LENGTH = 600
	SLIDE_LENGTH = 600
	print len(sys.argv)
	sc = SparkContext(appName="PythonStreamingKafka")
	ssc = StreamingContext(sc, 15)
	ssc.checkpoint("/home/paladin/Desktop/checkpoint") 
	zkQuorum, topic = sys.argv[1:]
	kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
	lines = kvs.map(lambda x: x[1])
	lines = lines.window(WINDOW_LENGTH,SLIDE_LENGTH)
	t1 = millis()
	
	
	def getScore(scores):
		if len(scores) > 0:
			return scores[0]
		else:
			return 0

	def process(rdd):
		interFilenames = set()
		intraFilenames = set()
		intraMetricKeys = []
		interMetricKeys = []

		INTER_THRESHOLD = 0.075
		INTRA_THRESHOLD = 0.0372
		
		dataReorganized = DataReorganized('localhost', 27017)

		# intraMetrics = rdd.filter(lambda x: ',' not in str(x[0])).filter(lambda x: 'm' not in str(x[0])).filter(lambda x: x[1] > INTRA_THRESHOLD)
		# interMetrics = rdd.filter(lambda x: ',' in str(x[0])).filter(lambda x: x[1] > INTER_THRESHOLD)

		intraMetrics = rdd.filter(lambda x: ',' not in str(x[0])).filter(lambda x: 'm' not in str(x[0])).top(10, key=lambda x: x[1])
		interMetrics = rdd.filter(lambda x: ',' in str(x[0])).top(10, key=lambda x: x[1])

		with open("/home/paladin/Desktop/I", "a") as myfile:
			myfile.write(str(rdd.filter(lambda x: ',' not in str(x[0])).filter(lambda x: 'm' not in str(x[0])).top(10, key=lambda x: x[1])))
			myfile.write(str(rdd.filter(lambda x: ',' in str(x[0])).top(10, key=lambda x: x[1])))
		allMeters = rdd.filter(lambda x: 'meterid' in str(x[0])).map(lambda x: x[1]).collect()
		allTimestamps = rdd.filter(lambda x: 'timestamp' in str(x[0])).map(lambda x: x[1]).collect()
		allMeasurements = rdd.filter(lambda x: 'measurement' in str(x[0])).map(lambda x: x[1]).collect()
		
		meterScore = getScore(allMeters)
		timestampScore = getScore(allTimestamps)
		measurementScore = getScore(allMeasurements)
		
		for intraMetric in intraMetrics:
			intraMetricKeys.append(((intraMetric.replace('\(','').split(','))[0]).strip())
		# intraMetricKeys = intraMetrics.map(lambda x: x[0]).collect()
		for k in intraMetricKeys:
			intraFilenames.add(k)
		
		for interMetric in interMetrics:
			interWords = interMetric.replace('\)').replace('\(').split(',')
			interMetricKeys.append((interWords[0]).strip())
			interMetricKeys.append((interWords[1]).strip())
		# interMetricKeys = interMetrics.map(lambda x: x[0]).flatMap(lambda x: x).collect()
		for k in interMetricKeys:
			interFilenames.add(k)

		filenames = list(intraFilenames & interFilenames)

		with open("/home/paladin/Desktop/K","a") as myfile:
					myfile.write(str(filenames) +" " +str(meterScore) + " "+ str(measurementScore) +" " + str(timestampScore)+ "\n")

		if len(filenames) > 0:
			if getMaxScore(meterScore, timestampScore, measurementScore) == 0:
				filenamesTimestamp = filter(lambda x: 'time' in x, filenames)
				filenamesMeasurement = filter(lambda x: 'mea' in x, filenames)
				filenames = filenamesMeasurement + filenamesTimestamp
				dataReorganized.reorganizeByMeterId(filenames, "/data-re/")
				with open("/home/paladin/Desktop/C","a") as myfile:
					myfile.write("meter score: " +str(meterScore) + "\n")
			elif getMaxScore(meterScore, timestampScore, measurementScore) == 1:
				filenames = filter(lambda x: 'time' not in x, filenames)
				dataReorganized.reorganizeByTimestamp(filenames, "/data-re/")
				with open("/home/paladin/Desktop/C", "a") as myfile:
					myfile.write("Reorganize by timestamp" +"\n")
			elif getMaxScore(meterScore, timestampScore, measurementScore) == 2:
				filenames = filter(lambda x: 'mea' not in x, filenames)
				dataReorganized.reorganizeByMeasurement(filenames, "/data-re/")
				with open("/home/paladin/Desktop/C", "a") as myfile:
					myfile.write("Reorganize by measurement"+"\n")
			else:
				print "do nothing"

	def mapper1(line):
		words = ast.literal_eval(line)
		
		word = words[0].split(";")

		if word[0] == 'query1':
			yield ('meterid', (0, float(word[4])))
			yield ('timestamp', (0, float(word[4])))
			yield ('measurement', (float(word[4]), float(word[4])))
		elif word[0] == 'query2':
			yield ('meterid', (0, float(word[4])))
			yield ('timestamp', (float(word[4]), float(word[4])))
			yield ('measurement', (float(word[4]), float(word[4])))
		elif word[0] == 'query3':
			yield ('meterid', (0, float(word[4])))
			yield ('timestamp', (float(word[4]), float(word[4])))
			yield ('measurement', (float(word[4]), float(word[4])))
		elif word[0] == 'query4':
			yield ('meterid', (0, float(word[4])))
			yield ('timestamp', (0, float(word[4])))
			yield ('measurement', (float(word[4]), float(word[4])))
		elif word[0] == 'query5':
			yield ('meterid', (0, float(word[4])))
			yield ('timestamp', (float(word[4]), float(word[4])))
			yield ('measurement', (float(word[4]), float(word[4])))
		elif word[0] == 'query6':
			yield ('meterid', (0, float(word[4])))
			yield ('timestamp', (float(word[4]), float(word[4])))
			yield ('measurement', (float(word[4]), float(word[4])))
		else:
			yield ('meterid', (float(word[4]), float(word[4])))
			yield ('timestamp', (float(word[4]), float(word[4])))
			yield ('measurement', (0, float(word[4])))

		for word in words:
			w = word.split(";")
			yield (w[1], (float(w[2])*float(w[4])/float(w[3]), float(w[4])))
		
		if len(words) > 1:
			size = len(words) - 1
			for i in range(0, size):
				wi = words[i].split(";")
				for j in range(i + 1, size + 1):
					wj = words[j].split(";")
					yield ((wi[1], wj[1]),((float(wi[2])/float(wi[3]) + float(wj[2])/float(wj[3]))*float(wi[4]),float(wi[4])))

	def getMaxScore(meterid, timestamp, measurement):
		if meterid > timestamp and meterid > measurement:
			return 0
		elif timestamp > meterid and timestamp > measurement:
			return 1
		else:
			return 2

	metrics = lines.flatMap(mapper1).reduceByKeyAndWindow(lambda a, b: ((a[0] + b[0]),(a[1] + b[1])),WINDOW_LENGTH,SLIDE_LENGTH).mapValues(lambda c: c[0]/c[1]).foreachRDD(lambda rdd: process(rdd))
	
	t2 = millis()

	ssc.start()
	ssc.awaitTermination()

if __name__ == "__main__":
	main()