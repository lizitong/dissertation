from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from operator import add,sub
import time
#from sklearn.neighbors import BallTree
from pyspark.sql import SQLContext, Row

# localtime([seconds]) -> (tm_year,tm_mon,tm_day,tm_hour,tm_min,tm_sec,tm_wday,tm_yday,tm_isdst)
def parse(line):
	line = line.split(",")
	#lon = round(float(line[0]), 3)
	#lat = round(float(line[1]), 3)
	ballTree = ballTreeBC.value
	elements = elementsBC.value
	ind = ballTree.query_radius([[line[0],line[1]]], r=5e-04)
	keys = []
	for i in ind[0]:
		keys.append(tuple(elements[i]))
	"""
	timeSeconds = float(line[2])
	t = time.localtime(timeSeconds)
	hour = t[3]
	minute = t[4]
	weekday = t[6]
	"""
	#return ((lon, lat),[line[2],1])
	return [(key, [line[2],1]) for key in keys]

def myAdd(a, b):
	return [b[0], a[1]+b[1]]

def mySub(a, b):
	return [a[0], a[1]-b[1]]

#record: ((weekday, hour, minute, lon, lat), (count, pdt))
def calPercentage(record):
	count, pdt = record[1]
	if not pdt:
		return (record[0], 1)
	pdt = float(pdt) / 5
	percentage = (count - pdt) / pdt
	return (record[0], percentage)

#((lon, lat),[line[2],1])
#((weekday, dt.hour, minute, lon, lat), count)
def parseTime(record):
	timeSeconds = float(record[1][0])
	t = time.localtime(timeSeconds)
	hour = t[3]
	m = t[4]
	if t[5] > 30:
		m += 1
	minute = m - (m % 5)
	if (m % 5) > 2.5:
		minute += 5
	if minute == 60:
		minute = 0
	weekday = t[6]
	
	newR = ((weekday, hour, minute, record[0][0], record[0][1]), record[1][1])
	return newR

def myLeftOuterJoin(record, bcName):
	trafficPdt = bcName.value
	if record[0] not in trafficPdt:
		newValue = (record[1], None)
	else: newValue = (record[1], trafficPdt[record[0]])
	return (record[0], newValue)

#((lon, lat),[line[2],1])
def filterZeros(record):
	if record[1][1] == 0:
		return False
	else:
		return 	True

"""
def genBallTree():
	minX = -74.260
	maxX = -73.690
	minY = 40.477
	maxY = 40.918
	x = minX
	y = minY
	elements = []
	while x <= maxX:
		x = round(x,3)
		y = minY
		while y <= maxY:
			y = round(y,3)
			elements.append([x,y])
			y += 0.001
		x += 0.001
	ballTree = BallTree(elements, metric="haversine")
	return (ballTree, elements)
"""

def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']

def comparison(record,tableName):
	sqlc = getSqlContextInstance(rdd.context)
	sqlStm = "SELECT count FROM "+tableName+" WHERE lon == " +str(record[0][3])+ " AND lat == " +str(record[0][4])
	c = sqlContext.sql(sqlStm).first()
	if not c:
		return (record[0], 1)
	pdt = c.count
	pdt = float(pdt) / 5
	percentage = (record[1] - pdt) / pdt
	return (record[0], percentage)


def toRDD(row, firstRecord):
	t = tuple(row)
	key = firstRecord[0][0:3] + t[1:3]
	value = t[0]
	return (key, value)

#((weekday, dt.hour, minute, lon, lat), count)
def process(rdd):
	sc = rdd.context
	sqlc = getSqlContextInstance(sc)
	firstRecord = rdd.first()
	weekdayStr = str(firstRecord[0][0])
	hourStr = str(firstRecord[0][1])
	minuteStr = str(firstRecord[0][2])
	bcName = "trafficPdt_" + weekdayStr + "_" + hourStr + "_" + minuteStr
	try:
		trafficPdt = bcName.value
	except:
		parquetPath = "parquets/weekday="+weekdayStr+"/hour="+hourStr+"/minute="+minuteStr
		trafficPdtDF = sqlc.read.parquet(parquetPath)#.rdd.map(tuple).saveAsTextFile("df")
		trafficPdtRDD = trafficPdtDF.map(lambda x:toRDD(x,firstRecord)).collect()
		d = dict(trafficPdtRDD)
		bcName = sc.broadcast(d)
	
	rdd.map(lambda x: myLeftOuterJoin(x,bcName)).map(calPercentage)\
			.saveAsTextFile("output/output-"+weekdayStr+"-"+hourStr+"-"+minuteStr+"-"+str(rdd.id()))
	"""
	
	parquetPath = "parquets/weekday="+weekdayStr+"/hour="+hourStr+"/minute="+minuteStr
	trafficPdtDF = sqlc.read.parquet(parquetPath)
	tableName = "trafficPdt" + str(rdd.id())
	trafficPdtDF.registerTempTable(tableName)
	rdd.map(lambda x: comparison(x,tableName)).saveAsTextFile("output/output-"+weekdayStr+"-"+hourStr+"-"+minuteStr)
	sqlc.dropTempTable(tableName)
	"""
	

if __name__ == "__main__":
	conf = SparkConf()
	conf.setAppName('streaming-test')
	conf.setMaster('yarn-client')
	conf.set('spark.executor.cores','2')
	conf.set("spark.rpc.netty.dispatcher.numThreads","2")
	sc = SparkContext(conf=conf)
	#ss = SQLContext(sc)
	ssc = StreamingContext(sc, 5)
	"""
	trafficPdt = ssc.sparkContext.pickleFile("Obj1").collect()
	trafficPdt = dict(trafficPdt)
	trafficPdtBC = sc.broadcast(trafficPdt)
	"""
	#ballTree, elements = genBallTree()
	ballTree = pickle.load( open("/home/hduser/output/genBallTree/ballTree.p", "rb") )
	elements = pickle.load( open("/home/hduser/output/genBallTree/elements.p", "rb") )
	ballTreeBC = sc.broadcast(ballTree)
	elementsBC = sc.broadcast(elements)
	
	ssc.checkpoint("checkpoint")
	lines = ssc.socketTextStream("10.0.2.15", 9997)

	userRequests = lines.flatMap(parse).reduceByKeyAndWindow(myAdd, mySub, 60, 10).filter(filterZeros).map(parseTime)#.saveAsTextFiles("output/output")
	#result = userRequests.map(myLeftOuterJoin).map(calPercentage)
	result = userRequests.foreachRDD(process)

	ssc.start()
	ssc.awaitTermination()