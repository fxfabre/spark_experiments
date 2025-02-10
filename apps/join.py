"""
Improve / avoir shuffle join
Use spark bucketing (video 71 & 72)
~500k lines in each dataframe

1. bucket the dataset
    ie : save to DV with bucketBy
To try :
    - read DF, and join : should not do shuffle join, and should see "sort merge join"
    - Same with SQL ?

Large to large DF : shuffle join
Large to small DF : broadcast join

Optimize :
- Huge volumes : Understand the data, filter useless data
- limits on number of partitions :
    - number of executors : useless to have more partitions
    - number of partitions on the data
    - Number of unique values on the join keys : 1 exchange for 1 join key
- Add join key if 1 partition is too big (key skew)
- broadcast(df) to force broadcast
- Bucketing dataset : require a shuffle, but occur only once
    - task "sortMergeJoin"

Disable broadcast join :
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

"""

import random
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

GENERATE_DF = False


def get_df_orders(spark, nb_products):
    """
    "order_id", "product_id", "unit_price", "order_qty", "client_name"]
    """
    schema = StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), False),
            StructField("unit_price", IntegerType(), True),
            StructField("order_qty", IntegerType(), False),
            StructField("client_name", StringType(), True),
        ]
    )

    if GENERATE_DF:
        df_orders = spark.createDataFrame(
            [
                (
                    monotonically_increasing_id(),
                    product_id,
                    random.randint(10, 30_000),
                    random.randint(1, 5),
                    "Mr Leon Martin Dupont",
                )
                for product_id in range(1, nb_products + 1)
                for repeat in range(random.randint(0, 100) * 10)
            ],
            schema=schema,
        )

        (
            df_orders.write.format("parquet")
            .mode("overwrite")
            # .partitionBy("product_id")
            .option("path", "/opt/spark-data/join/orders/")
            .save()
        )
    else:
        df_orders = spark.read.format("parquet").load("/opt/spark-data/join/orders/")

    if df_orders.count() == 0:
        raise Exception("No orders found")

    df_orders.show(5)
    return df_orders


def get_df_products(spark, nb_products):
    """
    ["product_id", "product_name", list_price, "company_name", "stock_qty"]
    """
    schema = StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("list_price", IntegerType(), True),
            StructField("company_name", StringType(), True),
            StructField("stock_qty", IntegerType(), True),
        ]
    )

    if GENERATE_DF:
        df_products = spark.createDataFrame(
            [
                (
                    product_id,
                    f"the product name {product_id}",
                    random.randint(2, 200),
                    "the company name",
                    random.randint(0, 100_000),
                )
                for product_id in range(1, nb_products + 1)
            ],
            schema=schema,
        )
        (
            df_products.write.format("json")
            .mode("overwrite")
            # .partitionBy("product_id")
            .option("path", "/opt/spark-data/join/products/")
            .save()
        )
    else:
        df_products = (
            spark.read.format("json")
            .option("schema", schema)
            .load("/opt/spark-data/join/products/")
        )

    if df_products.count() == 0:
        raise Exception("No products found")

    df_products.show(5)
    return df_products


def shuffle_join():
    spark = SparkSession.builder.appName("join").getOrCreate()

    orders_list = [
        ("01", "02", 350, 1),
        ("01", "04", 580, 1),
        ("01", "07", 320, 2),
        ("02", "03", 450, 1),
        ("02", "06", 220, 1),
        ("03", "01", 195, 1),
        ("04", "09", 270, 3),
        ("04", "08", 410, 2),
        ("05", "02", 350, 1),
    ]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [
        ("01", "Scroll Mouse", 250, 20),
        ("02", "Optical Mouse", 350, 20),
        ("03", "Wireless Mouse", 450, 50),
        ("04", "Wireless Keyboard", 580, 50),
        ("05", "Standard Keyboard", 360, 10),
        ("06", "16 GB Flash Storage", 240, 100),
        ("07", "32 GB Flash Storage", 320, 50),
        ("08", "64 GB Flash Storage", 430, 25),
    ]

    product_df = spark.createDataFrame(product_list).toDF(
        "prod_id", "prod_name", "list_price", "qty"
    )

    product_df.show()
    order_df.show()

    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")

    order_df.join(product_renamed_df, order_df.prod_id == product_df.prod_id, "inner").drop(
        product_renamed_df.prod_id
    ).select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty").show()


def my_shuffle_join():
    spark = SparkSession.builder.appName("join").getOrCreate()

    df_orders = get_df_orders(spark, 10_000)
    df_products = get_df_products(spark, 100_000)

    df_orders.join(df_products, df_orders.product_id == df_products.product_id, "inner").drop(
        df_products.product_id
    ).select(
        "order_id",
        "product_id",
        "product_name",
        "unit_price",
        "list_price",
        "order_qty",
    ).show()


def broadcast_join():
    spark = SparkSession.builder.appName("join").getOrCreate()

    order_df = spark.createDataFrame(
        [
            ("01", "02", 350, 1),
            ("01", "04", 580, 1),
            ("01", "07", 320, 2),
            ("02", "03", 450, 1),
            ("02", "06", 220, 1),
            ("03", "01", 195, 1),
            ("04", "09", 270, 3),
            ("04", "08", 410, 2),
            ("05", "02", 350, 1),
        ]
    ).toDF("order_id", "prod_id", "unit_price", "qty")

    product_df = spark.createDataFrame(
        [
            ("01", "Scroll Mouse", 250, 20),
            ("02", "Optical Mouse", 350, 20),
            ("03", "Wireless Mouse", 450, 50),
            ("04", "Wireless Keyboard", 580, 50),
            ("05", "Standard Keyboard", 360, 10),
            ("06", "16 GB Flash Storage", 240, 100),
            ("07", "32 GB Flash Storage", 320, 50),
            ("08", "64 GB Flash Storage", 430, 25),
        ]
    ).toDF("prod_id", "prod_name", "list_price", "reorder_qty")

    product_df.show()
    order_df.show()

    order_df.join(broadcast(product_df), order_df.prod_id == product_df.prod_id, "inner").drop(
        product_df.prod_id
    ).select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty").show()


def my_broadcast_join():
    spark = SparkSession.builder.appName("join").getOrCreate()

    df_orders = get_df_orders(spark, 10_000)
    df_products = get_df_products(spark, 100_000)

    df_orders.join(
        broadcast(df_products), df_orders.product_id == df_products.product_id, "inner"
    ).drop(df_products.product_id).select(
        "order_id",
        "product_id",
        "product_name",
        "unit_price",
        "list_price",
        "order_qty",
    ).show()


def my_bucketing_join():
    spark = (
        SparkSession.builder.appName("join").master("local[2]").enableHiveSupport().getOrCreate()
    )
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.sql("CREATE DATABASE IF NOT EXISTS TEST_DB")

    get_df_orders(spark, 10_000).write.bucketBy(12, "product_id").mode("overwrite").saveAsTable(
        "TEST_DB.orders"
    )
    get_df_products(spark, 100_000).write.bucketBy(8, "product_id").mode("overwrite").saveAsTable(
        "TEST_DB.products"
    )
    time.sleep(5)

    df_orders = spark.read.table("TEST_DB.orders")
    df_products = spark.read.table("TEST_DB.products")
    join_df = df_orders.join(df_products, "product_id", "inner")
    join_df.show(7)


if __name__ == "__main__":
    my_bucketing_join()
    input("Press Enter to end...")
