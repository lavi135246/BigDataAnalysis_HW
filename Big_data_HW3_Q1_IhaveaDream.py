from pyspark import SparkContext
from pyspark import SparkConf
import time

sparkConf = SparkConf() \
        .setAppName("CountWord") \

sc = SparkContext(conf = sparkConf)
#print sc.master

text_file = sc.textFile("hdfs://nn/user/s31tsm61/IhaveaDream.txt")

s_time = time.time()
counts = text_file.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').replace('\'',' ').replace('\"',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)

#print counts.take(20)
e_time = time.time() - s_time



s_time1 = time.time()
imp_count = counts.filter(lambda x: len(x[1])>4 )
#print imp_count.take(10)
e_time1 = time.time() - s_time1

#print "Elapsed Time:" ,e_time+e_time1

a = sc.master
stri = "Elapse Time: "+str(e_time+e_time1)
timeRDD = sc.parallelize([a,stri])


#file_o = open("aouttt.txt","w")
#a = sc.master
#file_o.write(a)
#stri = "Elapse time: " + str(e_time+etime1)
#file_o.write(stri)
#file_o.close()

counts.saveAsTextFile("hdfs://nn/user/s31tsm61/outt")
timeRDD.saveAsTextFile("hdfs://nn/user/s32tsm61/aout")
