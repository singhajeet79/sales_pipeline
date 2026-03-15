from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, round

spark = SparkSession.builder.appName("Bronze-To-Silver").getOrCreate()

# Pathing
bronze_path = "s3a://bytevault-ade-bronze-ap-south-1/sales/sales.csv"
silver_path = "s3a://bytevault-ade-silver-ap-south-1/sales_cleaned/"

# Transformation
df = spark.read.option("header", "true").csv(bronze_path)

silver_df = df.withColumn("quantity", col("quantity").cast("int")) \
              .withColumn("price", col("price").cast("double")) \
              .withColumn("total_spent", round(col("quantity") * col("price"), 2)) \
              .withColumn("processed_at", current_timestamp())

# Write to Silver
silver_df.write.mode("overwrite").parquet(silver_path)
print("✅ Silver Layer Written Successfully")
spark.stop()
