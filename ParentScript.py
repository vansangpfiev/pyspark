import os
import time
import PySparkMain

import FilterExpression
from FilterExpression import FilterExpression
from FilterExpression import OperatorType

import sys


if os.path.exists('log/sexecTime'):
    os.remove('log/sexecTime')

# add filters to input parameteres 
#filter construct should be here

dir = 'hdfs://' + sys.argv[1] + ':8020/user/meterdata'
mnode = sys.argv[2]
print mnode


################
filters = [FilterExpression("measurement", "0.5", OperatorType.GREATER), FilterExpression("measurement", "0.9", OperatorType.LOWER)]
filters1 = []
PySparkMain.main("query1", dir, filters,filters1, mnode)
print "Executed Query 1"
time.sleep(5)

filters = [FilterExpression("measurement", "0.5", OperatorType.GREATER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-05-01 17:00:00", OperatorType.EQ)]
PySparkMain.main("query2", dir, filters,filters1, mnode)
print "Executed Query 2"
time.sleep(5)

filters = [FilterExpression("measurement", "0.4", OperatorType.GREATER), FilterExpression("measurement", "0.8", OperatorType.LOWER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-05-01 17:00:00", OperatorType.EQ)]
PySparkMain.main("query3", dir, filters,filters1, mnode)
print "Executed Query 3"
time.sleep(5)


filters = [FilterExpression("measurement", "0.85", OperatorType.LOWER), FilterExpression("measurement", "0.5", OperatorType.GREATER)]
filters1 = []
PySparkMain.main("query4", dir, filters,filters1, mnode)
print "Executed Query 4"
time.sleep(5)


filters = [FilterExpression("measurement", "0.5", OperatorType.GREATER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-05-01 17:00:00", OperatorType.EQ)]
PySparkMain.main("query5", dir, filters,filters1, mnode)
print "Executed Query 5"
time.sleep(5)


filters = [FilterExpression("meterid", "1", OperatorType.EQ)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-04-01 17:00:00", OperatorType.EQ)]
PySparkMain.main("query6", dir, filters, filters1, mnode)
print "Executed Query 6	"
time.sleep(5)


filters = [FilterExpression("meterid", "30", OperatorType.LOWER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-04-01 17:00:00", OperatorType.EQ)]
PySparkMain.main("query7", dir, filters, filters1, mnode)
print "Executed Query 7	"
time.sleep(5)

print ""

print "Done! logs are in log/execTime"
