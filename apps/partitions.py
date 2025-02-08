"""
Launch :
docker exec -it spark_experiments-spark-master-1 bash
./bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark-apps/partitions.py
"""

from pyspark.sql.functions import spark_partition_id

from common import Log4J, read_file


def simple_repartition():
    spark, df = read_file("re-partition", "temp_by_city_small.csv")
    logger = Log4J(spark, __name__)

    logger.info("Num partitions df", df.rdd.getNumPartitions())
    df.groupBy(spark_partition_id()).count().show()

    # region random repartition
    df_random = df.repartition(7)
    logger.info("Num partitions df_random", df_random.rdd.getNumPartitions())
    df_random.groupBy(spark_partition_id()).count().show()
    (
        df_random.write.format("parquet")
        .mode("overwrite")
        .option("path", "/opt/spark-data/repartition/")
        .save()
    )

    # region partition by
    (
        df.write.format("json")
        .mode("overwrite")
        .partitionBy("Country", "City")
        .option("maxRecordsPerFile", 10000)
        .option("path", "/opt/spark-data/partition_by/")
        .save()
    )


if __name__ == "__main__":
    simple_repartition()
