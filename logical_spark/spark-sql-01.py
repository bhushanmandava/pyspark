from pyspark.sql import SparkSession
from pyspark.sql import Row 
spark = SparkSession.builder.appName("Spark-sql-01").getOrCreate()#creating the spark sessions

def mapper(line):
    fields = line.split(",")
    return Row(ID = int(fields[0]),name = str(fields[1]).encode("utf-8"),age = int(fields[2]),numFriends =  int(fields[3]))
lines = spark.sparkContext.textFile("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/fakefriends.csv")
data = lines.map(mapper)
#creating of the data frame
schemaPeople = spark.createDataFrame(data).cache()
schemaPeople.createOrReplaceTempView("df")
#getting teenagers from the table using sql command
teenagers = spark.sql("select * from df where age>= 13 and age<20")
for teen in teenagers.collect():
    print(teen)
schemaPeople.groupBy("age").count().orderBy("age").show()
spark.stop()