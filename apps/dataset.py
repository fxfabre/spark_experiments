from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, Row


def main():
    spark = SparkSession.builder.master("local[1]").appName("Dataset").getOrCreate()

    schema = StructType(
        [
            StructField("col_id", IntegerType()),
            StructField("date", StringType()),
            StructField("Country", StringType()),
            StructField("temperature", IntegerType()),
        ]
    )
    rows = [
        Row(1, "1/10/2024", "France", 12),
        Row(2, "01/12/2024", "RU", 2),
        Row(4, "3/7/2025", "France", 30),
    ]
    rdd = spark.sparkContext.parallelize(rows, 2)
    df = spark.createDataFrame(rdd, schema)

    df.printSchema()
    df.show()

    new_df = df.withColumn("date", to_date(col("date"), "d/M/y"))
    new_df.printSchema()
    new_df.show()


if __name__ == "__main__":
    main()
