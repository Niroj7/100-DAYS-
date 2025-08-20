from pyspark.sql import SparkSession
    if __name__=='__main__':
    spark:SparkSession = SparkSessicdon.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    list1 =[1,2,3,4]
    rdd=spark.sparkContext.parallelize(list1)
    print(rdd.collect())


