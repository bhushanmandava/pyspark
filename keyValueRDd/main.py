# form pyspark impots SparkConf , SparkContext
from pyspark import SparkConf, SparkContext
#creating the conf for the spark context
conf = SparkConf().setMaster("local").setAppName("keyValueRDD")
sc = SparkContext(conf=conf)#created a spark context object
#now creating the text file 
lines = sc.textFile("/Users/bhushanchowdary/Documents/GitHub/pyspark/keyValueRDd/fakefriends.csv")
#now creating the key value pair rdd
#to begin with we will create a function that pics up the age and associated friends
def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])#taking age feild and casting that to int
    friends = int(fields[3])#taking friends feild and casting that to int
    return (age,friends)#returnig the tuple of age and friends
# now all ve have to do is map that function to the lines of rdd
ageFriends = lines.map(parseLine) #we got key value pair rdd
#now we can proceed to find the avg friends by age
avgbyage = ageFriends.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))#we are mapping the values to a tuple of (friends,1) and then reducing by key to get the sum of friends and count

avgbyage = avgbyage.mapValues(lambda x: x[0]/x[1])#now we have mapped the values to get the avg friends by age
#finall action we need to collect them
result = avgbyage.collect()#collecting the result   
#now we can print the result
for age , avg in result:
    print(f"Age: {age}, Avg Friends: {avg}")#printing the age and avg friends

