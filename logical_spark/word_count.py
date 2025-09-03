from pyspark import SparkConf , SparkContext
import re #we are also importing regex for our text identification and separtion

conf = SparkConf().setMaster("local").setAppName("wordcount")
sc = SparkContext(conf=conf)
lines =sc.textFile("/Users/bhushanchowdary/Documents/GitHub/pyspark/datasets/book.txt")
#now what do need to do normilize the words of each line
def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())
normalizedWords = lines.flatMap(normalizeWords)
wordsCounts = normalizedWords.map(lambda x:(x,1)).reduceByKey(lambda x,y : x+y)
results = wordsCounts.map(lambda x:(x[1],x[0])).sortByKey().collect()


for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)