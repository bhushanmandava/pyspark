from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
spark = SparkSession.builder.appName("mostPoupular super hero").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
data = spark.read.option("sep", " ").schema(schema).csv("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/Marvel+Names.txt")
lines = spark.read.text("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/Marvel+Graph.txt")
connections = lines.withColumn("id", fun.split(fun.col("value"), " ")[0]) \
                   .withColumn("connections", fun.size(fun.split(fun.col("value"), " ")) - 1) \
                   .groupBy("id").agg(fun.sum("connections").alias("connections"))
min_Connections_count = connections.agg(fun.min("connections")).first()[0]
minConnections = connections.filter(fun.col("connections")==min_Connections_count)
#now we got all the ids and min connections list
#getting ythe name 
res = minConnections.join(data,"id")
res.show(10)