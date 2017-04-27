from pyspark import SparkContext
from pyspark import SparkConf
import time
import numpy as np

sparkConf = SparkConf() \
		.setAppName("CountMean") \

sc = SparkContext(conf = sparkConf)

def f_path(num):
    path = "hdfs://nn/user/s31tsm61/yellow_tripdata_2015-"
    if (num < 10):
        path += (str(0) + str(num) + ".csv")
        print path
    else:
        path += (str(num) + ".csv")
        print path
    return path

def PrepareData(sc, num):
    print("Loading Data")
    path = f_path(num)
    rawDataWithHeader = sc.textFile(path)
    header = rawDataWithHeader.first()
    rawData = rawDataWithHeader.filter(lambda x: x!=header)
    lines = rawData.map(lambda x: x.split(","))
    #print(lines.first())
    #print(header)
    print("Total: " + str(lines.count()) + " lines of data")
    return lines, header

my_sum = np.zeros(5)
my_cnt = np.zeros(5)
linesofdata = np.zeros(12)
	

s_time = time.time()	
	
for i in range(1,13):
    frameRDD, _ = PrepareData(sc, i)
    linesofdata[(i-1)] = frameRDD.count()
    droppedRDD = frameRDD.filter(lambda x: x[3]!='0')
    sumRDD = droppedRDD.map(lambda x: (x[11], float(x[3]))) \
        .reduceByKey(lambda accum, n: accum+n) \
        .sortByKey() \
        .collect()
    cntRDD = droppedRDD.map(lambda x: (x[11], 1)) \
        .reduceByKey(lambda accum, n: accum+n) \
        .sortByKey() \
        .collect()
    
    for i in range(0,5):
        try:
            print str(sumRDD[i][0])+ " " + str(float(sumRDD[i][1])/float(cntRDD[i][1]))
            my_sum[i] += sumRDD[i][1]
            my_cnt[i] += cntRDD[i][1]
        except IndexError:
            print("Exception")
            pass

e_time = time.time() - s_time			

a = my_sum/my_cnt
resultRDD = sc.parallelize(a.tolist())
time_out = []
time_out.append(e_time)
timeRDD = sc.parallelize(time_out)

resultRDD.saveAsTextFile("hdfs://nn/user/s31tsm61/Resultt")
timeRDD.saveAsTextFile("hdfs://nn/user/s31tsm61/Resultttime")
