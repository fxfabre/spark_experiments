# Spark sandbox

## Run some tests :
- ./bin/run-example --master spark://spark-master:7077 --name spark-pi SparkPi 10
- ./bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-apps/word_frequency.py
- ./bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-apps/kaggle_global_warming.py

## refs
- https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b
