
#--- download multi files and upload on HDFS---#
for i in {1..12}; do wget "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-`printf "%02d" $i`.csv"; done
for i in {1..12}; do hadoop -put "yellow_tripdata_2015-`printf "%02d" $i`.csv"; done
#for i in {1..12}; do echo "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2015-`printf "%02d" $i`.csv"; done

#/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit WordCount.py



#--- spark submit ---#
spark-submit --master yarn --deploy-mode cluster {file_name}.py
spark-submit --master yarn --deploy-mode client {file_name}.py
spark-submit --master local[*] {file_name}.py




#--- delete folder ---#
hadoop fs -ls -R {file_name}



#--- ipython with spark ---#
export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark/

#pyspark shell does not support yarn-cluster mode
export PYSPARK_SUBMIT_ARGS="--master yarn --deploy-mode client pyspark-shell"
export PYSPARK_SUBMIT_ARGS="--master local[*] pyspark-shell"

ipython notebook --profile=pyspark


#--- useful link for spark submit ---#
####-------http://spark.apache.org/docs/latest/submitting-applications.html