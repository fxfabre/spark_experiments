from collections import namedtuple

from pyspark import SparkConf
from pyspark.sql import SparkSession

from common import Log4J

APP_NAME = "SparkContext.sample"

CitiesTempEvol = namedtuple("CitiesTempEvol", ["country", "city", "delta_temp"])


def main():
    conf = (
        SparkConf()
        # .setMaster("local[3]")  # Or set it from command line --master spark://spark-master:7077
        .set("spark.driver.memory", "2g")
        .setAppName(APP_NAME + ".python")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4J(spark, "rdd_sample")
    sc = spark.sparkContext

    rdd = (
        sc.textFile("/opt/spark-data/csv/temp_evolution_by_city_tiny.csv")
        .repartition(2)
        .map(lambda line: line.replace('"', "").split(","))
        .map(lambda line: CitiesTempEvol(line[0], line[1], float(line[2])))
        .filter(lambda line: line.delta_temp > 0)
        .map(lambda line: (line.country, line.delta_temp))
        .reduceByKey(lambda temp1, temp2: temp1 + temp2)
    )
    for res in rdd.collect():
        logger.info(str(res))
