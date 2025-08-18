from pyspark.sql import SparkSession, functions as F, types as T

# -------------------------------
# 1) Load + clean + write tables
# -------------------------------
def gathering_cleaning(
    spark,
    product_txt="file:///home/takeo/product.txt",
    sales_txt="file:///home/takeo/sales.txt",
    db="iphone_store",
    product_table="product",
    sales_table="sales"
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
    spark.sql(f"USE `{db}`")

    # Product: read text -> cast -> dedupe -> parquet Hive table
    df_product = (spark.read
                  .option("header", "true")
                  .option("delimiter", "|")
                  .csv(product_txt))
    df_product = (df_product
                  .withColumn("product_id", F.col("product_id").cast(T.IntegerType()))
                  .withColumn("product_name", F.col("product_name").cast(T.StringType()))
                  .withColumn("unit_price", F.col("unit_price").cast(T.IntegerType()))
                  .dropDuplicates(["product_id"]))
    (df_product.write
     .mode("overwrite")
     .format("parquet")
     .saveAsTable(f"`{db}`.`{product_table}`"))

    # Sales: read text -> cast -> filter -> parquet Hive table partitioned by sale_date
    df_sales = (spark.read
                .option("header", "true")
                .option("delimiter", "|")
                .csv(sales_txt))
    df_sales = (df_sales
                .withColumn("seller_id", F.col("seller_id").cast(T.IntegerType()))
                .withColumn("product_id", F.col("product_id").cast(T.IntegerType()))
                .withColumn("buyer_id", F.col("buyer_id").cast(T.IntegerType()))
                .withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
                .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
                .withColumn("price", F.col("price").cast(T.IntegerType()))
                .dropna(subset=["product_id", "quantity", "sale_date", "price"])
                .dropDuplicates())

    (df_sales.write
     .mode("overwrite")
     .format("parquet")
     .partitionBy("sale_date")
     .saveAsTable(f"`{db}`.`{sales_table}`"))

    return f"{db}.{product_table}", f"{db}.{sales_table}"

# -------------------------------
# 2) Simple EDA tables (optional)
# -------------------------------
def perform_EDA(spark, sales_table, product_table, db="iphone_store"):
    spark.sql(f"USE `{db}`")
    df_sales = spark.table(sales_table)
    df_product = spark.table(product_table)

    df = df_sales.join(df_product, on="product_id", how="left")

    daily_sales = (df.groupBy("sale_date")
                   .agg(F.sum(F.col("quantity") * F.col("price")).alias("total_sales"),
                        F.sum("quantity").alias("units_sold"))
                   .orderBy("sale_date"))

    daily_tbl = f"{db}.daily_sales_summary"
    (daily_sales.write.mode("overwrite").format("parquet")
     .saveAsTable("daily_sales_summary"))  # current DB, so unqualified is fine

    peak_days = daily_sales.orderBy(F.desc("total_sales")).limit(5)
    (peak_days.write.mode("overwrite").format("parquet")
     .saveAsTable("peak_sales_days"))  # FIX: plural + matches return

    return f"{db}.daily_sales_summary", f"{db}.peak_sales_days"

# -------------------------------------------------------
# 3) REQUIRED: buyers who bought S8 but NOT iPhone table
# -------------------------------------------------------
def data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table):
    products = spark.table(product_hive_table)
    sales = spark.table(sales_hive_table)

    sp = (sales.alias("s")
          .join(products.alias("p"), F.col("s.product_id") == F.col("p.product_id"), "inner")
          .select(F.col("s.buyer_id").alias("buyer_id"),
                  F.col("p.product_name").alias("product_name")))

    buyers_s8 = sp.filter(F.col("product_name") == F.lit("S8")).select("buyer_id").distinct()
    buyers_iphone = sp.filter(F.col("product_name") == F.lit("iPhone")).select("buyer_id").distinct()

    result = buyers_s8.join(buyers_iphone, on="buyer_id", how="left_anti")

    (result.select("buyer_id")
     .write.mode("overwrite").format("parquet")
     .saveAsTable(target_hive_table))

    return target_hive_table

# -------------------------------
# 4) Main (spark-submit entry)
# -------------------------------
if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("GatheringCleaningJob")
             # point warehouse to a writable local folder
             .config("spark.sql.warehouse.dir", "file:/home/takeo/spark_warehouse")
             .config("hive.metastore.warehouse.dir", "file:/home/takeo/spark_warehouse")
             # embedded Derby metastore path — MAKE SURE /home/takeo exists
             .config("javax.jdo.option.ConnectionURL",
                     "jdbc:derby:;databaseName=/home/takeo/metastore_db;create=true")
             .enableHiveSupport()
             .getOrCreate())

    # Make sure the local files exist at these paths (or change them):
    product_tbl, sales_tbl = gathering_cleaning(
        spark,
        product_txt="file:///home/takeo/product_cel.txt",
        sales_txt="file:///home/takeo/sale_cel.txt",
        db="iphone_store",
        product_table="product",
        sales_table="sales"
    )

    daily_tbl, peak_tbl = perform_EDA(spark, sales_tbl, product_tbl, db="iphone_store")

    # Create the REQUIRED final table
    buyers_tbl = data_preparation_api(
        spark,
        product_hive_table=product_tbl,
        sales_hive_table=sales_tbl,
        target_hive_table="buyers_s8_not_iphone"
    )

    print("Created tables:")
    print("  Product table  :", product_tbl)
    print("  Sales table    :", sales_tbl)
    print("  Daily summary  :", daily_tbl)
    print("  Peak days      :", peak_tbl)
    print("  Buyers (final) :", buyers_tbl)

    # Quick verify
    spark.table(buyers_tbl).show()
    spark.stop()
