from pyspark import SparkConf,SparkContext
#this example is all about filtering in RDD
#creating the conf file
conf = SparkConf().setMaster("local").setAppName("FilteringRDD")
#creating the spark context
sc = SparkContext(conf = conf)
#creatign the text file
lines = sc.textFile("/Users/bhushanchowdary/Documents/GitHub/pyspark/Filtering_with_RDD/1800.csv")
#now we will parse that date and get the ones we need
def parseLine(lines):
    fields = lines.split(",")
    id = fields[0] # extracting the id and convetring that to int
    weather = fields[2]
    temp = float(fields[3])#extarctibg teh temperature and convertinfg that to the flo9at
    return (id, weather, temp)  #returning the tuple of id, weather and temp
#now we will map this function to the lines of rdd -> working dataset
weatherData = lines.map(parseLine)
#now we will filter the data to get the ones we need the min temperature
minTemp = weatherData.filter(lambda x: x[1] =='TMIN')
#now we are only concesdring the stations and their min te
stationTemp = minTemp.map(lambda x: (x[0], x[2]))  #mapping the station id and temp
minimumStationTemp = stationTemp.reduceByKey(lambda x,y : min(x,y))
#now collecting the result
result = minimumStationTemp.collect()
#now printing the resut
for station, temp in result:
    print(f"Station: {station}, Min Temp: {temp}")  #printing the station and min temperature