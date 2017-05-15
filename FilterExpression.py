from enum import Enum


class FilterExpression : 
    
    def __init__(self, left, right, op) :
        self.left = left
        self.right = right
        self.operator = op     

    def getLeft(self) :
        return self.left       

    def getRight(self) :
        return self.right
    
    def getOperator(self) :
        return self.operator 


class OperatorType(Enum) :
    GREATER = "$gt"
    GREATEREQ = "$ge"
    LOWER = "$lt"
    LOWEREQ = "$le"
    NOTEQ = "$ne"
    EQ = "$eq"  
