export SPARK_HOME=/opt/cloudera/parcels/CDH/lib/spark/

#---- local -----#
export PYSPARK_SUBMIT_ARGS="--master local[*] pyspark-shell"

#---- yarn-client ----#
export PYSPARK_SUBMIT_ARGS="--master yarn --deploy-mode client pyspark-shell"

ipython notebook --profile=pyspark