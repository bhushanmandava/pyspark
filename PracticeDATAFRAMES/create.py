from pyspark.sql import SparkSession
from pyspark.sql import StructType,StructField,StringType
spark = SparkSession.builder.appName('sparkcreate').getOrCreate()
emptyRDD = spark.sparkContext.emptyRDD()#from creating an empty rdd
#anotheer way to do that is to parllelize
rdd2 = spark.sparkContext.parallelize([])
## ** creation of the pyspark DATA Frame ***

schema = StructType([
    StructField('firstname',StringType(),True),
    StructField('middlename',StringType(),True),
    StructField('lastname',StringType(),True),
])
# now we have a Schema we can build a data frame using createDataFrame() using that Schema
df = spark.createDataFrame(emptyRDD,schema)
df1 = emptyRDD.toDF(schema)
df2 = spark.createDataFram([],StructType([]))