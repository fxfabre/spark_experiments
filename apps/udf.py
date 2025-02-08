from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, expr, monotonically_increasing_id
from pyspark.sql.types import StringType


def capitalize(s: str):
    return s.capitalize()


def create_df_1(spark):
    return spark.createDataFrame(
        [
            {"name": "john", "city": "lonDon"},
            {"name": "deLphine", "city": "lonDon"},
            {"name": "toTO", "city": "lonDon"},
        ]
    ).withColumn("id", monotonically_increasing_id())  # auto increment


def create_df_2(spark):
    return spark.createDataFrame(
        [
            (1, "john", "lonDon"),
            (2, "deLphine", "lonDon"),
            (3, "toTO", "lonDon"),
        ]
    ).toDF("id", "name", "city")


def main():
    spark = SparkSession.builder.appName("udf").getOrCreate()
    df = create_df_2(spark)
    df.printSchema()
    df.show()

    # Python DataFrame UDF
    capitalize_udf = udf(capitalize, StringType())
    df2 = df.withColumn("name", capitalize_udf(col("name")))
    df2.show()

    # SQL UDF
    spark.udf.register("capitalize_udf", capitalize, StringType())
    print(
        "Check registration",
        [f for f in spark.catalog.listFunctions() if "_udf" in f.name],
    )
    df3 = df.withColumn("city", expr("capitalize_udf(city)"))
    df3.show()


if __name__ == "__main__":
    main()
