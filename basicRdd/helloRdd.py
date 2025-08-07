from pyspark import SparkConf

if  __name__ == "__main__":
    conf = SparkConf()\
    .setMaster("local[3]")\
    .setAppName("HelloRDD")

# sc = SparkContext(conf=conf)

spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()
sc = spark.sparkContext
logger = Log4j(spark)
if len(sys.argv)!=2:
    logger.error("usage:hello spark <filename>")
linesRDD = sc.textFile(sys.argv[1])
