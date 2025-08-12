from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, StructType as ST

# Start Spark
sc = SparkContext("local", "AssignmentQ1_Q2_Q4_Q5")
spark = SparkSession(sc)

# ----------------------------
# Q1) RDD with 5 partitions
# ----------------------------
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd = sc.parallelize(data, 5)
print("\nQ1) Number of partitions:", rdd.getNumPartitions())

# ----------------------------
# Q2) Read text file and count lines
# ----------------------------
rdd2 = sc.textFile("data.txt")  # make sure data.txt is in the same folder
print("Q2) Total number of records:", rdd2.count())

# ----------------------------
# Q4) DataFrame -> register view Users; salary > 3000
# ----------------------------
data_q4 = [
    ("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
]
schema_q4 = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])
df_q4 = spark.createDataFrame(data_q4, schema_q4)
df_q4.createOrReplaceTempView("Users")

print("\nQ4) Salary > 3000")
spark.sql("SELECT * FROM Users WHERE salary > 3000").show()

# ----------------------------
# Q5) Nested struct -> view Users; firstname where lastname='Rose'
# ----------------------------
structureData = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
]
structureSchema = StructType([
    StructField('name', ST([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('id', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

df_nested = spark.createDataFrame(structureData, structureSchema)
df_nested.createOrReplaceTempView("Users")

print("Q5) Firstname where lastname = 'Rose'")
spark.sql("SELECT name.firstname AS firstname FROM Users WHERE name.lastname = 'Rose'").show()

# Stop Spark
spark.stop()
