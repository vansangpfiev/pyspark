import pymongo
from pymongo import MongoClient

import FilterExpression
from FilterExpression import FilterExpression
from FilterExpression import OperatorType


class MongoIndex :
    
    def __init__(self, host, port) :
      self.client = MongoClient(host, port)
      self.db = self.client.indexes
    

    def getFileNames(self, ddir, filters) :
        collection = self.db.meterdata
        result = ""
        res = ""  
        if (len(filters) == 1) :
            right = filters[0].getRight()
            if isnumber(right) : 
                if right.isdigit() : 
                    right = int(right)
                else :
                    right = float(right)
                
            left = filters[0].getLeft()
            print left + '     and    ' + filters[0].getOperator() + '        and      ' + str(right) 
            res = collection.find({left : { filters[0].getOperator() : right}}).distinct("fname")  
        else : 
            if (len(filters) == 2) :
                right1 = filters[0].getRight()
                left1 = filters[0].getLeft()
                right2 = filters[1].getRight()
                left2 = filters[1].getLeft() 
            
                if isnumber(right1) : 
                    if right1.isdigit() : 
                        right1 = int(right1)
                    else :
                        right1 = float(right1)
                if isnumber(right2) : 
                    if right2.isdigit() : 
                        right2 = int(right2)
                    else :
                        right2 = float(right2)
                 
                if(left1 == left2) : 
                    res = collection.find({left1 : {filters[0].getOperator() : right1, filters[1].getOperator() : right2 }}).distinct("fname")
                else :
                    res = collection.find({left1 : {filters[0].getOperator() : right1}, left2 : {filters[1].getOperator() : right2}}).distinct("fname") 
        for f in res : 
            result = result + ddir + "/" + f + ","
        return result[:len(result)-1] 


def isnumber(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

