from pyspark import SparkConf, SparkContext, SparkFiles
from operator import add
from datetime import datetime
#from sklearn.neighbors import BallTree
from calendar import monthrange
from pyspark.sql import SQLContext, Row
import pickle

APP_NAME = "OBJ1"

def deleteInvalidLines(line):
	try:
		int(line[0])
	except:
		return False
	return line[9] != "0.0" and line[10] != "0.0"

#15/1/2015 19:23
#2015-01-25 00:13:06
#Only use hour and minute, converte minute to intervals of 5.
def parse(line):
  	dt = datetime.strptime(line[1],  "%Y-%m-%d %H:%M:%S")
	weekday = dt.weekday() #Monday is 0 and Sunday is 6
	#module = dt.minute % 5
	m = dt.minute
	if dt.second > 30:
		m += 1
	minute = m - (m % 5)
	if (m % 5) > 2.5:
		minute += 5
	if minute == 60:
		minute = 0
	
	lon = float(line[5])
	lat = float(line[6])

	newLine = (weekday, dt.hour, minute, lon, lat)
	return newLine

def mergeValue(a,b):
	a.append(b)
	return a

def mergeCombiners(a,b):
	a.extend(b)
	return a

def countWeekdays(line):
	#line = line.split(",")
	dt = datetime.strptime(line[1],  "%Y-%m-%d %H:%M:%S")
	numWeekdays = [4,4,4,4,4,4,4]
	firstWeekday, numDays = monthrange(dt.year, dt.month)
	for i in range(numDays-28):
		numWeekdays[(firstWeekday+i)%7] += 1
	return numWeekdays

def calAverage(record):
	nums = numWeekdays.value
	weekday = record[0][0]
	n = float(record[1]) / nums[weekday]
	return (record[0], n)

def filterZeros(record):
	if record[1] == 0:
		return False
	else:
		return 	True

def findNeighors(row):
	ballTree = ballTreeBC.value
	elements = elementsBC.value
	ind = ballTree.query_radius([[row.lon,row.lat]], r=5e-04)
	rs = []
	for i in ind[0]:
		key = (row.weekday, row.hour, row.minute, elements[i][0], elements[i][1])
		rs.append((key,1))
	
	return rs

def main(sc):
	pass

if __name__ == "__main__":
	conf = SparkConf()
	conf.setAppName(APP_NAME)
	conf.setMaster('yarn-client')
	sc = SparkContext(conf=conf)
	ss = SQLContext(sc)

	#ballTree, elements = genBallTree()
	ballTree = pickle.load( open("/home/hduser/output/genBallTree/ballTree.p", "rb") )
	elements = pickle.load( open("/home/hduser/output/genBallTree/elements.p", "rb") )
	ballTreeBC = sc.broadcast(ballTree)
	elementsBC = sc.broadcast(elements)

	csvPaths = []
	#csvPaths.append("/taxidata/green/green_tripdata_2015-01.csv")
	#csvPaths.append("/taxidata/green/test.csv")
	for i in range(1, 13):
		if i < 10:
			month = "0"+str(i)
		else: month = str(i)
		#green_tripdata_2015-01.csv
		path = "/taxidata/green/green_tripdata_2015-" + month + ".csv"
		csvPaths.append(path)
		path2 = "/taxidata/yellow/yellow_tripdata_2015-" + month + ".csv"
		csvPaths.append(path2)
	#allResults = sc.emptyRDD()
	for path in csvPaths:
		#rdd = sc.textFile(path)
		trips = sc.textFile(path).map(lambda line:line.split(",")).filter(deleteInvalidLines)
		numWeekdays = countWeekdays(trips.first())
		numWeekdays = sc.broadcast(numWeekdays) 
		#lambda p: Row(name=p[0], age=int(p[1])) (weekday, dt.hour, minute, lon, lat)
		
		parsedTrips = trips.map(parse).map(lambda p: Row(weekday=p[0], hour=p[1], minute=p[2], lon=p[3], lat=p[4]))
		df = ss.createDataFrame(parsedTrips).cache()
		
		parsedTrips = trips.map(parse).cache()
		for weekday in range(7):
			for hour in range(24):
				
				subDf = df.filter((df.weekday==weekday) & (df.hour==hour))
				if(subDf.rdd.isEmpty()): continue
				subrdd = subDf.rdd.flatMap(findNeighors).reduceByKey(add)\
							.map(lambda r: Row(weekday=r[0][0], hour=r[0][1], minute=r[0][2], lon=r[0][3], lat=r[0][4], count=r[1]))
				resultDf = ss.createDataFrame(subrdd).write.partitionBy('weekday','hour','minute').mode('append').parquet("parquets")
				
				#subrdd = parsedTrips.filter(lambda r: r[0]==weekday and r[1]==hour)
		
		numWeekdays.unpersist()
	#numWeekdays = [4,4,4,4,4,4,4]
	#numWeekdays = sc.broadcast(numWeekdays)
	#main(sc)