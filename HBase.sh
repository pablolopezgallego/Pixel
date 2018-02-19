#!/bin/bash -x
(cd /root/borrar3/Pixel/; sbt package)
spark-submit --class streaming --master yarn-cluster --packages it.nerdammer.bigdata:spark-hbase-connector_2.10:1.0.3,org.apache.spark:spark-streaming-kafka_2.10:1.6.2 --conf spark.yarn.appMasterEnv.ENTORNO=PRODUCCION --executor-memory 2G --driver-memory 4G --num-executors 4 --executor-cores 1 --driver-cores 4 /root/borrar3/Pixel/target/scala-2.10/spark_2.10-1.0.jar /sanitas/taxonomias/taxonomias/Taxonomias.csv /sanitas/taxonomias/dictVarSanitas/dictVarSanitas.txt /sanitas/taxonomias/dictTax/DictTax.csv
