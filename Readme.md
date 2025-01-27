# Spark sandbox

## Setup project
- `wget https://raw.githubusercontent.com/mrn-aglic/pyspark-playground/refs/heads/main/book_data/pride-and-prejudice.txt -O data/books/pride-and-prejudice.txt`

## Run some tests :
- ./bin/run-example --master spark://spark-master:7077 --name spark-pi SparkPi 10
- ./bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-apps/word_frequency.py

## refs
- https://medium.com/@MarinAgli1/setting-up-a-spark-standalone-cluster-on-docker-in-layman-terms-8cbdc9fdd14b
