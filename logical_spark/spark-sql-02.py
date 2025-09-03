from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("spark-sql-02").getOrCreate()
people = spark.read.option("header","true").option("inferSchema","true").csv("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/fakefriends-header.csv")
print("printing the whole Schema")
people.printSchema()
print("lets display name coloumn")
people.select("name").show()# alwasy end with action
print("filtering pople between 21")
people.select("*").where("age<21").show()
print("grouoyy and counting ")
people.groupBy("age").count().show()
print("make evry one 10 years older")
people.select(people.name ,expr("age+10 as after_10")).show()
spark.stop()