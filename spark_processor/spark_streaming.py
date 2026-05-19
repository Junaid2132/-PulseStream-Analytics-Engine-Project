from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- CONFIGURATION ---
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "ecommerce-clicks"

def main():
    print("⚡ Starting PulseStream Spark Processor Engine...")

    spark = SparkSession.builder \
        .appName("PulseStreamAnomalyDetector") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Flexible Schema Definition
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("action", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("country", StringType(), True)
    ])

    # 3. Kafka Stream Read
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Binary Stream to JSON Conversion
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), schema).alias("data")) \
        .select("data.*")

    # 5. SAFE ANOMALY DETECTION LOGIC (Checking if amount column exists and is high)
    processed_df = json_df.withColumn(
        "is_anomaly",
        when(col("amount") > 5000.0, 1).otherwise(0)
    )

    # 6. Live Output to Console Terminal
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()