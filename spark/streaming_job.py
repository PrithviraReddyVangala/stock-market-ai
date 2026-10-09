from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, round
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("StockMarketStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Define schema of Kafka stock data
# --------------------------------------------------

stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("previous_close", DoubleType(), True),
    StructField("change", DoubleType(), True),
    StructField("percent_change", DoubleType(), True),
    StructField("received_at", StringType(), True)
])

# --------------------------------------------------
# 3. Read data from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock-market")
    .option("startingOffsets", "latest")
    .load()
)

# --------------------------------------------------
# 4. Convert Kafka value from binary to string
# --------------------------------------------------

json_df = kafka_df.select(
    col("value").cast("string").alias("json_data")
)

# --------------------------------------------------
# 5. Convert JSON into columns
# --------------------------------------------------

stock_df = (
    json_df
    .select(
        from_json(col("json_data"), stock_schema).alias("data")
    )
    .select("data.*")
)

# --------------------------------------------------
# 6. Calculate stock metrics
# --------------------------------------------------

from pyspark.sql.functions import when, round

metrics_df = (
    stock_df
    .filter(col("price").isNotNull())
    .filter(col("previous_close").isNotNull())
    .filter(col("open").isNotNull())
    .filter(col("high").isNotNull())
    .filter(col("low").isNotNull())

    # Percentage change from previous close
    .withColumn(
        "price_change_pct",
        round(
            ((col("price") - col("previous_close"))
             / col("previous_close")) * 100,
            2
        )
    )

    # Difference between day's high and low
    .withColumn(
        "price_range",
        round(col("high") - col("low"), 2)
    )

    # Percentage change from today's opening price
    .withColumn(
        "intraday_change_pct",
        round(
            ((col("price") - col("open"))
             / col("open")) * 100,
            2
        )
    )

    # Market direction
    .withColumn(
        "market_direction",
        when(col("price") > col("previous_close"), "UP")
        .when(col("price") < col("previous_close"), "DOWN")
        .otherwise("FLAT")
    )
)

# --------------------------------------------------
# 7. Write each micro-batch to MySQL
# --------------------------------------------------

def write_to_mysql(batch_df, batch_id):

    (
        batch_df
        .select(
            "symbol",
            "timestamp",
            col("price"),
            col("open").alias("open_price"),
            col("high").alias("high_price"),
            col("low").alias("low_price"),
            "volume",
            "previous_close",
            col("change").alias("change_value"),
            "percent_change",
            "received_at",
            "price_change_pct",
            "price_range",
            "intraday_change_pct",
            "market_direction"
        )
        .write
        .format("jdbc")
        .option(
            "url",
            "jdbc:mysql://localhost:3306/stock_market"
        )
        .option("dbtable", "stock_stream")
        .option("user", "root")
        .option("password", "prthvi123")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .mode("append")
        .save()
    )


# --------------------------------------------------
# 8. Start Spark Streaming
# --------------------------------------------------

query = (
    metrics_df
    .writeStream
    .foreachBatch(write_to_mysql)
    .outputMode("append")
    .option("checkpointLocation", "checkpoints/stock_market")
    .start()
)

# --------------------------------------------------
# 9. Keep Spark running
# --------------------------------------------------

query.awaitTermination()