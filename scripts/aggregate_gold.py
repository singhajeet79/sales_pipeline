from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, current_timestamp

spark = SparkSession.builder.appName("Silver-To-Gold").getOrCreate()

# Pathing
silver_path = "s3a://bytevault-ade-silver-ap-south-1/sales_cleaned/"
gold_path = "s3a://bytevault-ade-gold-ap-south-1/sales_summary/"

# Load Silver
silver_df = spark.read.parquet(silver_path)

# Transformation (Aggregation)
gold_df = silver_df.groupBy("product") \
                   .agg(sum("total_spent").alias("total_revenue")) \
                   .withColumn("report_date", current_timestamp())

# Write to Gold
gold_df.write.mode("overwrite").parquet(gold_path)
print("✅ Gold Layer Written Successfully")
spark.stop()
