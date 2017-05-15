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

#filters = [FilterExpression("measurement", "0.57", OperatorType.GREATER), FilterExpression("meterid", "2000", OperatorType.LOWER)]
#PySparkMain.main("query5", dir, filters, mnode)
#print "Executed Query 5"
  

filters = [FilterExpression("meterid", "1", OperatorType.EQ)]
PySparkMain.main("query6", dir, filters, mnode)
print "Executed Query 6"
