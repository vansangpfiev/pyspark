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

filters = [FilterExpression("meterid", "5", OperatorType.GREATER),FilterExpression("meterid", "300", OperatorType.LOWER)]
PySparkMain.main("query1", dir, filters, mnode)
print "Executed Query 1"
time.sleep(5)

filters = [FilterExpression("meterid", "5", OperatorType.GREATER),FilterExpression("meterid", "300", OperatorType.LOWER)]
PySparkMain.main("query1", dir, filters, mnode)
print "Executed Query 1"
time.sleep(5)



filters = [FilterExpression("meterid", "150", OperatorType.GREATER),FilterExpression("meterid", "1200", OperatorType.LOWER)]
PySparkMain.main("query2", dir, filters, mnode)
print "Executed Query 2"
time.sleep(5)

filters = [FilterExpression("meterid", "150", OperatorType.GREATER),FilterExpression("meterid", "1200", OperatorType.LOWER)]
PySparkMain.main("query2", dir, filters, mnode)
print "Executed Query 2"
time.sleep(5)



filters = [FilterExpression("meterid", "5", OperatorType.GREATER),FilterExpression("meterid", "300", OperatorType.LOWER)]
PySparkMain.main("query3", dir, filters, mnode)
print "Executed Query 3"
time.sleep(5)

filters = [FilterExpression("meterid", "5", OperatorType.GREATER),FilterExpression("meterid", "300", OperatorType.LOWER)]
PySparkMain.main("query3", dir, filters, mnode)
print "Executed Query 3"
time.sleep(5)



filters = [FilterExpression("measurement", "0.85", OperatorType.GREATER)]
PySparkMain.main("query4", dir, filters, mnode)
print "Executed Query 4"
time.sleep(5)

filters = [FilterExpression("measurement", "0.85", OperatorType.GREATER)]
PySparkMain.main("query4", dir, filters, mnode)
print "Executed Query 4" 
time.sleep(5)



filters = [FilterExpression("measurement", "0.57", OperatorType.GREATER), FilterExpression("meterid", "2000", OperatorType.LOWER)]
PySparkMain.main("query5", dir, filters, mnode)
print "Executed Query 5" 
time.sleep(5)


filters = [FilterExpression("measurement", "0.57", OperatorType.GREATER), FilterExpression("meterid", "2000", OperatorType.LOWER)]
PySparkMain.main("query5", dir, filters, mnode)
print "Executed Query 5"
time.sleep(5)



filters = [FilterExpression("meterid", "1", OperatorType.EQ)]
PySparkMain.main("query6", dir, filters, mnode)
print "Executed Query 6"
time.sleep(5)

filters = [FilterExpression("meterid", "1", OperatorType.EQ)]
PySparkMain.main("query6", dir, filters, mnode)
print "Executed Query 6"
time.sleep(5)

#filters = [FilterExpression("meterid", "1", OperatorType.EQ)]
#PySparkMain.main("query6", dir, filters, mnode)
#print "Executed Query 6"
#time.sleep(10)

filters = [FilterExpression("meterid", "0", OperatorType.GREATER), FilterExpression("meterid", "300", OperatorType.LOWER)]
PySparkMain.main("query7", dir, filters, mnode)
print "Executed Query 7"
time.sleep(5)

filters = [FilterExpression("meterid", "0", OperatorType.GREATER), FilterExpression("meterid", "300", OperatorType.LOWER)]
PySparkMain.main("query7", dir, filters, mnode)
print "Executed Query 7"


print ""

print "Done! logs are in log/execTime"
