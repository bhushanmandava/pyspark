import sys
from pyspark.sql import *
from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf  = get_spark_conf()
    #what is this ? we are getting the confiuration form the spark.conf
    #now spark session creation
    spark = SparkSession\
            .builder\
            .appName("hellow_world")\
            .master("local[2]")\
            .getOrCreate()
    logger = Log4j(spark)
    if len(sys.argv)!=2:
        logger.error("usage : helloSpark<filename>")
        sys.exit(-1)
    logger.info("starting hellospark")
    survey_raw_df = load_survey_df(spark,sys.argv[1])
    partitioned_survey_df = survey_raw_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    count_df.show()

    logger.info("Finished Hellow wordl")
    spark.stop()

    