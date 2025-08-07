from pyspark import SparkConf, SparkContext
import collections
# that is whre we are importing all of our required imports
# Initialize Spark context
#conf is what we are creating all of or configuration for out program
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)#creating the sc for creating the RDD

# Load the correct data file
#comman way to create the RDD is to use sc.textFile -> spling up our data
lines = sc.textFile("/Users/bhushanchowdary/Documents/pyspark/movie-rating/ml-100k/u.data")

# Extract the rating (3rd column in each line)
# this way to get the 3rd coloumn form the each and every row from our RDD
#beacuse textFile creates string we are using .split() where it splits at white space
ratings = lines.map(lambda x: x.split()[2])#note lines rdd is immutable so we are assiging it to new thing called ratings


# Count occurrences of each rating 
# we are using action method called .countByValue()-> to get number of each rating came
result = ratings.countByValue()

# Sort and display the results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
