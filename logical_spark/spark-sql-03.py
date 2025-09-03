from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
spark= SparkSession.builder.appName("spark-sql-03").getOrCreate()
data = spark.read.option("header","true").option("inferSchema","true").csv("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/fakefriends-header.csv")
print("schema")
data.printSchema()

# goal - is to caluclate the avg friends for each age f=group

results = data.groupBy("age").agg(expr("avg(friends) as avg_friends")).orderBy("avg_friends")
results.show()
spark.stop()