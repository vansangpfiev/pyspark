class queries :
    def __init__(self, filepath) :

        self.query1 = "SELECT SUM(measurement)  FROM " + filepath + " WHERE measurement > 0.5 AND measurement < 0.9" 

        self.query2 = "SELECT SUM(measurement)  FROM " + filepath + " WHERE measurement > 0.5"\
        				+ " AND date > timestamp '2013-03-01 17:00:00' AND date < timestamp '2013-05-01 17:00:00'"

        self.query3 = "SELECT SUM(measurement)  FROM " + filepath + " WHERE measurement > 0.6 AND measurement < 1.0"\
        				+ " AND date > timestamp '2013-03-01 17:00:00' AND date < timestamp '2013-05-01 17:00:00'"

        self.query4 = "SELECT id, meterid, date, measurement  FROM " + filepath \
        			 	+ " WHERE measurement < 0.85 AND measurement > 0.5 ORDER BY meterid"

        self.query5 = "SELECT id, meterid, date, measurement  FROM " + filepath + " WHERE  measurement > 0.5  "\
        				+ "AND date > timestamp '2013-03-01 17:00:00' AND date < timestamp '2013-05-01 17:00:00' ORDER BY meterid"
        
        self.query6 = "SELECT SUM(hr)+94.96 FROM (SELECT SUM(measurement)*0.1114 AS hr FROM " + filepath \
        				+ " WHERE meterid = 1 AND hour(date) IN (1,2,3,4,5,6,7,15,16,17)"\
        				+ " UNION ALL SELECT SUM(measurement)*0.16 AS hr FROM " + filepath \
        				+ " WHERE meterid = 1 AND hour(date) NOT IN (1,2,3,4,5,6,7,15,16,17))"
        
        self.query7 = "SELECT meterid, SUM(hr) FROM (SELECT meterid, SUM(measurement)*0.1114+94.96 AS hr FROM " + filepath \
        				+ " WHERE meterid > 0 AND meterid < 30 AND hour(date) IN (1,2,3,4,5,6,7,15,16,17) GROUP BY meterid"\
        				+ " UNION ALL SELECT meterid, SUM(measurement)*0.16 AS hr FROM " + filepath \
        				+ " WHERE meterid > 0 AND meterid < 30 AND hour(date) NOT IN (1,2,3,4,5,6,7,15,16,17) GROUP BY meterid)"\
        				+ "	GROUP BY meterid"

