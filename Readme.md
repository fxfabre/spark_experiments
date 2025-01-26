# Spark sandbox

## Setup project
- `wget https://raw.githubusercontent.com/mrn-aglic/pyspark-playground/refs/heads/main/book_data/pride-and-prejudice.txt -O data/books/pride-and-prejudice.txt`

## Run some tests :
- cd /opt/spark/bin
- ./spark-submit --master spark://0.0.0.0:7077 --name spark-pi --class org.apache.spark.examples.SparkPi  local:///opt/spark/examples/jars/spark-examples_2.12-3.4.0.jar 100
- ./spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-apps/word_frequency.py
