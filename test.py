import os
import time
import PySparkMain

import FilterExpression
from FilterExpression import FilterExpression
from FilterExpression import OperatorType
from Constant import Constant


# if os.path.exists('log/execTime'):
#     os.remove('log/execTime')

# add filters to input parameteres 
#filter construct should be here 
filters = [FilterExpression("measurement", "0.5", OperatorType.GREATER), FilterExpression("measurement", "0.9", OperatorType.LOWER)]
filters1 = []
# filters1 = []
PySparkMain.main("query1", Constant.HDFS_DIR, filters,filters1, "localhost")
print "Executed Query 1"

filters = [FilterExpression("measurement", "0.5", OperatorType.GREATER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-05-01 17:00:00", OperatorType.EQ)]
# filters1 = []
PySparkMain.main("query2", Constant.HDFS_DIR, filters,filters1, "localhost")
print "Executed Query 2"

filters = [FilterExpression("measurement", "0.4", OperatorType.GREATER), FilterExpression("measurement", "0.8", OperatorType.LOWER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-05-01 17:00:00", OperatorType.EQ)]
# filters1 = []
PySparkMain.main("query3", Constant.HDFS_DIR, filters,filters1, "localhost")
print "Executed Query 3"

filters = [FilterExpression("measurement", "0.85", OperatorType.LOWER), FilterExpression("measurement", "0.5", OperatorType.GREATER)]
filters1 = []
# filters1 = []
PySparkMain.main("query4", Constant.HDFS_DIR, filters,filters1, "localhost")
print "Executed Query 4"

filters = [FilterExpression("measurement", "0.5", OperatorType.GREATER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-05-01 17:00:00", OperatorType.EQ)]
# filters1 = []
PySparkMain.main("query5", Constant.HDFS_DIR, filters,filters1, "localhost")
print "Executed Query 5"

filters = [FilterExpression("meterid", "1", OperatorType.EQ)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-04-01 17:00:00", OperatorType.EQ)]
# filters1 = []
PySparkMain.main("query6", Constant.HDFS_DIR, filters, filters1, "localhost")
print "Executed Query 6	"

filters = [FilterExpression("meterid", "30", OperatorType.LOWER)]
filters1 = [FilterExpression("date", "2013-03-01 17:00:00", OperatorType.EQ),FilterExpression("date", "2013-04-01 17:00:00", OperatorType.EQ)]
# filters1 = []
PySparkMain.main("query7", Constant.HDFS_DIR, filters, filters1, "localhost")
print "Executed Query 7	"
