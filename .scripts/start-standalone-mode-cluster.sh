start-master.sh
start-worker.sh -c 3 -m 9G spark://34.228.246.130:7077

spark-submit --class taxitrip.TaxiTrip \
--master spark://172.31.90.84:7077 \
target/scala-2.12/taxitripapp_2.12-0.1.jar \
--total-executor-cores 3 \
--executor-memory 9G


spark-shell --executor-memory 9G --driver-memory 9G    
spark-shell --master spark://172.31.90.84:7077