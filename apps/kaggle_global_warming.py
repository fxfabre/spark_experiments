"""
Data from Kaggle challenge :
https://www.kaggle.com/datasets/ankushpanday1/global-warming-dataset-195-countries-1900-2023
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql import Row
from pyspark.sql.window import Window
from pprint import pprint

APP_NAME = Path(__file__).stem.replace("_", " ")

def get_df(spark: SparkSession):
    return (
        spark
        .read.csv("/opt/spark-data/global_warming_dataset.csv", header=True, inferSchema=True)
        .select("Country", "Year", "Average_Temperature")
        .groupBy("Year", "Country").agg(F.avg("Average_Temperature").alias("Avg_Temperature"))
    )


def create_dataframe():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    df = spark.createDataFrame([
        Row(name="Alice", age=34),
        Row(name="Bob", age=45),
        Row(name="Charlie", age=29)
    ])

    df.show()


def display():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    df = spark.read.csv("/opt/spark-data/global_warming_dataset.csv", header=True, inferSchema=True)
    df.show(3)
    df.printSchema()
    df.filter("Country = 'Country_187'").select("Country", "Year", "Average_Temperature").groupBy("Country", "Year").count().sort("count", ascending=False).show()


def evol_temp_par_pays_python():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    window = Window.partitionBy("Country").orderBy("Year")
    df = (
        get_df(spark)
        .filter("Country >= 'Country_98'")
        .withColumn("previous_temp", F.lag("Avg_Temperature").over(window))
    )
    df.show()

def evol_temp_par_pays_sql():
    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    (
        spark
        .read.csv("/opt/spark-data/global_warming_dataset.csv", header=True, inferSchema=True)
        .createOrReplaceTempView("global_warming_dataset")
    )
    query = """
        SELECT *
        FROM global_warming_dataset
        WHERE Country = 'Country_98'
          AND Year == 1900
    """
    pprint(spark.sql(query).toPandas().T, width=160)



# display()
evol_temp_par_pays_python()
# evol_temp_par_pays_sql()
