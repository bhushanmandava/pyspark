from pyspark.sql import SparkSession
from pyspark.sql import functions as fun
from pyspark.sql.types import StructType , StructField, IntegerType , LongType
import codecs

def loadMovieName():
    movieName  = {}
    with codecs.open("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/ml-100k/u.item","r",encoding="ISO-8859-1",errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieName[int(fields[0])] = fields[1]
    return movieName
spark = SparkSession.builder.appName("Movie_with_df_02").getOrCreate()
name_Dict_brod = spark.sparkContext.broadcast(loadMovieName())
#defining the schema
schema = StructType(  
    [
        StructField("userId",IntegerType(),True),\
        StructField("movieId",IntegerType(),True),\
        StructField("rating",IntegerType(),True),\
        StructField("timestamp",LongType(),True),\
    ]
)
data = spark.read.option("sep","\t").schema(schema).csv("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/ml-100k/u.data")

movieCounts = data.groupBy("movieId").count()

def lookupname(movieId):
    return name_Dict_brod.value[movieId]
# registering the udf
lookupudf = fun.udf(lookupname)
movie_name = movieCounts.withColumn("movieTitle",lookupudf(fun.col("movieId")))
sortedMoviesWithName = movie_name.orderBy(fun.desc("count"))
sortedMoviesWithName.show(10)
spark.stop()
