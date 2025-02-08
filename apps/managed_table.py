from pyspark.sql import SparkSession

from common import get_logger


def main():
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("managed_table")
        .enableHiveSupport()
        .getOrCreate()
    )
    logger = get_logger(spark, "managed_table")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/spark-data/csv/temp_by_city.csv")
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS KAGGLE_DATA")
    spark.catalog.setCurrentDatabase("KAGGLE_DATA")

    # region Version 1, too many partitions
    # (
    #     df.write.mode("overwrite")
    #     .partitionBy("Country", "City")
    #     .saveAsTable("KAGGLE_DATA.temp_by_city")
    # )

    # region Version 2 : BucketBy to limit number of partitions, CSV to see file content
    (
        df.write.mode("overwrite")
        .format("csv")
        .bucketBy(8, "Country", "City")
        .saveAsTable("KAGGLE_DATA.temp_by_city")
    )

    for table in spark.catalog.listTables("KAGGLE_DATA"):
        logger.info(table, ": nb elements", table.count("*"))


if __name__ == "__main__":
    main()
