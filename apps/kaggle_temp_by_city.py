"""
Data from Kaggle challenge :
https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data/data

Launch :
docker exec -it spark_experiments-spark-master-1 bash
./bin/spark-submit --deploy-mode client /opt/spark-apps/kaggle_temp_by_city.py
"""

import logging
from pathlib import Path
from pprint import pprint

import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from common import Log4J

logging.getLogger("org.apache.spark.scheduler.TaskSetManager").setLevel(logging.WARNING)

APP_NAME = Path(__file__).stem


def display() -> None:
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    logger = Log4J(spark)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/spark-data/csv/temp_by_city.csv")
    )
    df.show(3)
    df.printSchema()
    logger.info("CSV schema : " + df.schema.simpleString())


def evol_temp_par_pays_python() -> pd.DataFrame:
    conf = (
        SparkConf()
        # .setMaster("local[3]")  # Or set it from command line --master spark://spark-master:7077
        .set("spark.driver.memory", "2g")
        .setAppName(APP_NAME + ".python")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    window = Window.partitionBy("Country", "City", "month").orderBy("dt_year")
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/spark-data/csv/temp_by_city.csv")
        .filter(F.col("AverageTemperature").isNotNull())
        # .filter("Country == 'France'")
        #
        # clean
        .select("dt", "Country", "City", "AverageTemperature")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("AverageTemperature", "avg_temperature")
        #
        .withColumn("dt", F.to_date(F.col("dt"), "yyyy-mm-dd"))
        .withColumn("dt_year", F.year(F.col("dt")))
        .withColumn("month", F.month(F.col("dt")))
        #
        # compute diff temp, same month, over years
        .withColumn("prev_year_temp", F.lag("avg_temperature").over(window))
        .withColumn("prev_dt", F.lag("dt").over(window))
        .withColumn("prev_year", F.lag("dt_year").over(window))
        .withColumn(
            "temp_diff_by_year",
            (F.col("avg_temperature") - F.col("prev_year_temp"))
            / (F.col("dt_year") - F.col("prev_year")),
        )
        .withColumn("temp_diff_by_year", F.round(F.col("temp_diff_by_year"), 5))
        .filter(F.col("temp_diff_by_year").isNotNull())
        .select("country", "city", "dt", "temp_diff_by_year")
        #
        # aggregate by contry
        .groupBy("country", "city")
        .agg(F.avg("temp_diff_by_year").alias("avg_diff_temp_by_year"))
        .orderBy("avg_diff_temp_by_year")
        .cache()
    )

    df.printSchema()
    pdf = df.toPandas()
    pprint(pdf, width=160)
    return pd.DataFrame(pdf)


def evol_temp_par_pays_sql() -> pd.DataFrame:
    query = Path("/opt/spark-apps/sql/evol_temp_par_pays.sql").read_text()

    spark = SparkSession.builder.appName(APP_NAME + ".sql").getOrCreate()
    (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/spark-data/csv/temp_by_city.csv")
        .createOrReplaceTempView("temp_by_city")
    )
    df = spark.sql(query).toPandas()
    pprint(df, width=160)
    return pd.DataFrame(df)


def main():
    # display()
    df_sql = evol_temp_par_pays_python()
    df_sql.to_csv("/opt/spark-data/out_evol_temp_par_pays_python.csv", index=False)
    df_py = evol_temp_par_pays_sql()
    df_py.to_csv("/opt/spark-data/out_evol_temp_par_pays_sql.csv", index=False)

    df = df_sql.merge(df_py, how="outer", on=["country", "city"]).assign(
        diff=lambda _df: _df["avg_diff_temp_by_year_x"] - _df["avg_diff_temp_by_year_y"]
    )
    df_diffs = df[df["diff"].abs() > 0.001]
    if len(df_diffs) > 0:
        print("Diffs found :", df_diffs)
    else:
        print("No diffs found")


if __name__ == "__main__":
    main()
