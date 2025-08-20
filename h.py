from pyspark.sql import SparkSession

# Step 1: Start SparkSession with Hive support
spark = SparkSession.builder \
    .appName("iPhone Sales Analysis") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

# Step 2: Read Parquet files from HDFS
sales_df = spark.read.parquet("hdfs:///user/takeo/input/sale_cel.parquet")
product_df = spark.read.parquet("hdfs:///user/takeo/input/product_cel.parquet")

# Step 3: Register DataFrames as temp views
sales_df.createOrReplaceTempView("sales")
product_df.createOrReplaceTempView("product")

# Step 4: Run SQL queries like Hive
result = spark.sql("""
    SELECT p.product_name, SUM(s.quantity * s.price) as total_sales
    FROM sales s
    JOIN product p
      ON s.product_id = p.product_id
    GROUP BY p.product_name
""")

# Step 5: Show results
result.show()

# Step 6: Save results back to Hive (optional)
result.write.mode("overwrite").saveAsTable("iphone.sales_summary")

