from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.types import StructType , StructField, IntegerType , LongType

spark = SparkSession.builder.appName("movies_with_df").getOrCreate()
#now beacuse our data set dossent have any schema we are going to createa schema for our table with coulumn names
schema = StructType(  
    [
        StructField("userId",IntegerType(),True),\
        StructField("movieId",IntegerType(),True),\
        StructField("rating",IntegerType(),True),\
        StructField("timestamp",LongType(),True),\
    ]
)
data = spark.read.option("sep","\t").schema(schema).csv("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/ml-100k/u.data")
mostRatedMovies = data.groupBy("movieId").count().orderBy(fun.desc("count"))
mostRatedMovies.show(10)
spark.stop()