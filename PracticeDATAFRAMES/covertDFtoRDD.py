from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("convertTordd-df").getOrCreate()
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parllelize(dept)
df=rdd.toDF()#without coloumns name
deptColumns = ["dept_name","dept_id"]
df1 = rdd.toDF(deptColumns)#this will contatin the similar column names
# we can create a new df
df2 = spark.createDataFrame(rdd,schema = deptColumns)



