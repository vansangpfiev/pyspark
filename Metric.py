from pymongo import MongoClient

import FilterExpression
from FilterExpression import FilterExpression
from FilterExpression import OperatorType


class Metric:
    def __init__(self, host, port):
        self.client = MongoClient(host, port)
        self.db = self.client.indexes

    def getMetricFactors(self, querytype, filenames, filterIdx, filterTmp):
        collectionIdx = self.db.meterdata
        collectionTmp = self.db.histogram
        colNames = filenames.split(",")
        fnames = []
        result = 0
        metricFactors = []
        for i in range(0, len(colNames)):
            print colNames[i]
            colName = colNames[i].split("/")
            
            fnames.append(colName[len(colName)-1])
        if len(filterTmp) == 0:
        	if len(filterIdx) == 1:
	            right = filterIdx[0].getRight()
	            if isNumber(right):
	                if right.isdigit():
	                    right = int(right)
	                else:
	                    right = float(right)
	            left = filterIdx[0].getLeft()

	            for value in fnames:           
	                resultIdx = collectionIdx.find({"fname": str(value), left: {filterIdx[0].getOperator(): right}}).count()	                
	                nRowsOfFile = collectionIdx.find({"fname": value}).count()	                
	                result += resultIdx
	                metricFactors.append(str(querytype) + ";" +str(value) + ";" + str(resultIdx) + ";" + str(nRowsOfFile))
	           	
	            metricFactor = [s + ";"+str(result) for s in metricFactors]
	            

	        else:
	            if len(filterIdx) == 2:
	                right1 = filterIdx[0].getRight()
	                left1 = filterIdx[0].getLeft()
	                right2 = filterIdx[1].getRight()
	                left2 = filterIdx[1].getLeft()
	                
	                if isNumber(right1):
	                    if right1.isdigit():
	                        right1 = int(right1)
	                    else:
	                        right1 = float(right1)
	                if isNumber(right2):
	                    if right2.isdigit():
	                        right2 = int(right2)
	                    else:
	                        right2 = float(right2)
	                
	                for value in fnames:
	                    if left1 == left2:
	                        resultIdx = collectionIdx.find({"fname": value, left1: {filterIdx[0].getOperator(): right1, 
	                            filterIdx[1].getOperator(): right2}}).count()
	                    else:
	                        resultIdx = collectionIdx.find({"fname": value, left1: {filterIdx[0].getOperator(): right1},
	                                                        left2: {filterIdx[1].getOperator(): right2}}).count()
	                    
	                    nRowsOfFile = collectionIdx.find({"fname": value}).count()
	                    
	                    result += resultIdx

	                    metricFactors.append(str(querytype) + ";" + str(value) + ";" + str(resultIdx) + ";" + str(nRowsOfFile))

	                print result
	                metricFactor = [s + ";"+str(result) for s in metricFactors]
        else:
	        if len(filterIdx) == 1:
	            right = filterIdx[0].getRight()
	            if isNumber(right):
	                if right.isdigit():
	                    right = int(right)
	                else:
	                    right = float(right)
	            left = filterIdx[0].getLeft()

	            startDate = filterTmp[0].getRight()
	            endDate = filterTmp[1].getRight()
	            for value in fnames:
	                
	                resultIdx = collectionIdx.find({"fname": str(value), left: {filterIdx[0].getOperator(): right}}).count()

	                resSDate = collectionTmp.find({"hist.date": startDate, "name": value}, {"hist": {
	                    "$elemMatch": {"date": {filterTmp[0].getOperator(): startDate}}}})
	                resEDate = collectionTmp.find({"hist.date": endDate, "name": value}, {"hist": {
	                    "$elemMatch": {"date": {filterTmp[1].getOperator(): endDate}}}})

	                resultTmp = abs((resSDate[0]['hist'])[0]['cumulation'] - (resEDate[0]['hist'])[0]['cumulation'])

	                nRowsOfFile = collectionIdx.find({"fname": value}).count()

	                nRows = resultIdx if resultIdx < resultTmp else resultTmp

	                result += nRows

	                metricFactors.append(str(querytype) + ";" +str(value) + ";" + str(nRows) + ";" + str(nRowsOfFile))

	            metricFactor = [s + ";"+str(result) for s in metricFactors]
	           	 
	        
	        else:
	            if len(filterIdx) == 2:
	                right1 = filterIdx[0].getRight()
	                left1 = filterIdx[0].getLeft()
	                right2 = filterIdx[1].getRight()
	                left2 = filterIdx[1].getLeft()
	                if isNumber(right1):
	                    if right1.isdigit():
	                        right1 = int(right1)
	                    else:
	                        right1 = float(right1)
	                if isNumber(right2):
	                    if right2.isdigit():
	                        right2 = int(right2)
	                    else:
	                        right2 = float(right2)

	                startDate = filterTmp[0].getRight()
	                endDate = filterTmp[1].getRight()
	                for value in fnames:
	                    if left1 == left2:
	                        resultIdx = collectionIdx.find({"fname": value, left1: {filterIdx[0].getOperator(): right1,
	                            filterIdx[1].getOperator(): right2}}).count()
	                    else:
	                        resultIdx = collectionIdx.find({"fname": value, left1: {filterIdx[0].getOperator(): right1},
	                        	left2: {filterIdx[1].getOperator(): right2}}).count()

	                    resSDate = collectionTmp.find({"hist.date": startDate, "name": value}, {"hist": {
	                    "$elemMatch": {"date": {filterTmp[0].getOperator(): startDate}}}})
	                    
	                    resEDate = collectionTmp.find({"hist.date": endDate, "name": value}, {"hist": {
	                    "$elemMatch": {"date": {filterTmp[1].getOperator(): endDate}}}})
	                
	                    
	                    resultTmp = abs((resSDate[0]['hist'])[0]['cumulation'] - (resEDate[0]['hist'])[0]['cumulation'])


	                    nRowsOfFile = collectionIdx.find({"fname": value}).count()

	                    nRows = resultIdx if resultIdx < resultTmp else resultTmp

	                    result += nRows

	                    metricFactors.append(str(querytype) + ";" +str(value) + ";" + str(nRows) + ";" + str(nRowsOfFile))

	                metricFactor = [s + ";"+str(result) for s in metricFactors]
       
        return metricFactor


def isNumber(value):
    try:
        float(value)
        return True
    except ValueError:
        return False
