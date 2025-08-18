
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------
# a) sales_data_collector_api(spark, text_file_path)
# ---------------------------------------------------------------
def sales_data_collector_api(spark, text_file_path):
    """
    Ingest SALES data from a local TEXT file (pipe-delimited with header),
    cast to the correct schema, and publish to a Hive *partitioned* table
    in Parquet format. Uses sale_date as the partition column.
    """
    table_name = "sales_partitioned_parquet"
    df = (spark.read.option("header", "true").option("delimiter", "|").csv(text_file_path))
    sales_clean = (
        df
        .withColumn("seller_id",  F.col("seller_id").cast(T.IntegerType()))
        .withColumn("product_id", F.col("product_id").cast(T.IntegerType()))
        .withColumn("buyer_id",   F.col("buyer_id").cast(T.IntegerType()))
        .withColumn("sale_date",  F.to_date(F.col("sale_date"), "yyyy-MM-dd"))
        .withColumn("quantity",   F.col("quantity").cast(T.IntegerType()))
        .withColumn("price",      F.col("price").cast(T.IntegerType()))
        .filter(F.col("sale_date").isNotNull() & F.col("buyer_id").isNotNull())
    )
    (sales_clean.write.format("parquet").mode("overwrite").partitionBy("sale_date").saveAsTable(table_name))
    return table_name

# ---------------------------------------------------------------
# b) product_data_collector_api(spark, parquet_file_path)
# ---------------------------------------------------------------
def product_data_collector_api(spark, parquet_file_path):
    """
    Ingest PRODUCT data from a Parquet file and publish to a NON-partitioned
    Hive table in Parquet format.
    """
    table_name = "product_nonpartitioned_parquet"
    df = spark.read.parquet(parquet_file_path)
    df = df.toDF(*[c.lower() for c in df.columns])
    product_clean = (
        df
        .withColumn("product_id",   F.col("product_id").cast(T.IntegerType()))
        .withColumn("product_name", F.col("product_name").cast(T.StringType()))
        .withColumn("unit_price",   F.col("unit_price").cast(T.IntegerType()))
        .select("product_id", "product_name", "unit_price")
        .dropDuplicates(["product_id"])
    )
    (product_clean.write.format("parquet").mode("overwrite").saveAsTable(table_name))
    return table_name

# ---------------------------------------------------------------
# c) data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table)
# ---------------------------------------------------------------
def data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table):
    """
    Build the analytics output table that reports DISTINCT buyers who bought
    'S8' but NOT 'iPhone'. Writes a single-column table (buyer_id).
    """
    products = spark.table(product_hive_table)
    sales = spark.table(sales_hive_table)
    sp = (sales.alias("s")
          .join(products.alias("p"), F.col("s.product_id") == F.col("p.product_id"), "inner")
          .select(F.col("s.buyer_id").alias("buyer_id"), F.col("p.product_name").alias("product_name")))
    buyers_s8 = sp.filter(F.col("product_name") == F.lit("S8")).select("buyer_id").distinct()
    buyers_iphone = sp.filter(F.col("product_name") == F.lit("iPhone")).select("buyer_id").distinct()
    result = buyers_s8.join(buyers_iphone, on="buyer_id", how="left_anti")
    (result.select("buyer_id").write.format("parquet").mode("overwrite").saveAsTable(target_hive_table))
    return target_hive_table

