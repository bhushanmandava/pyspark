#before converting that lets assess the key differences
'''
pandas ;_ runs it operation on a single node
PysparkDataframes : runs on multiple nodes and distribute the work among the node which allow 
parllel compution and make it faster to run
but MAchine Learning Algorithms needs Pandas 
so Process the data using pyspark then convert to pands and feed it to ml algorithms
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("topamds").getOrCreate()
data = [("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0)]
columns = ["first_name","middle_name","Last_name","dob","gender","Salary"]
df = spark.createDataFrame(data = data , Schema = columns)
pandasDf = df.toPandas()
