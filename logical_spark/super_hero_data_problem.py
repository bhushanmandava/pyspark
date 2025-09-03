from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("mostPoupular super hero").getOrCreate()

# Define schema for names
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Read Marvel names
data = spark.read.option("sep", " ").schema(schema).csv("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/Marvel+Names.txt")

# Read Marvel connections
lines = spark.read.text("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/Marvel+Graph.txt")

# Create connections table (minimal fix)
connections = lines.withColumn("id", fun.split(fun.col("value"), " ")[0]) \
                   .withColumn("connections", fun.size(fun.split(fun.col("value"), " ")) - 1) \
                   .groupBy("id").agg(fun.sum("connections").alias("connections"))

# Get the most popular superhero ID (minimal fix)
mostpopular = connections.sort(fun.col("connections").desc()).first()

# Get name corresponding to that ID
mostpopularName = data.filter(fun.col("id") == int(mostpopular[0])).select("name").first()

# Print result
print(mostpopularName)
