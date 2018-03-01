#OK
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
session <- sparkR.session(master = "yarn-client", appName = "SparkR_User_Mike", enableHiveSupport = TRUE)
#yarn 	Connect to a YARN cluster in client or cluster mode depending on the value of --deploy-mode.
# The cluster location will be found based on the HADOOP_CONF_DIR or YARN_CONF_DIR variable. 

# Check sparkR version
sparkR.version()
Sys.getenv("SPARK_HOME")


#OK
#sc <- sparkR.init()
#sqlContext <- sparkRSQL.init(sc)
#sqlContext
View(sql("SHOW TABLES"))
View(sql("DESCRIBE FORMATTED irisnortheu"))
res2df <- sql("select * from irisnortheu limit 10")
count(res2df)
cache(res2df)

dfLessThan3 <- sql("select * from irisnortheu where m1 < 5")
View(dfLessThan3)

dfClassCount <- sql("select flower, Count(1) as NoOf from irisnortheu group by flower")
View(dfClassCount)
head(dfClassCount)

# all flowers
alldf <- sql("select * from irisnortheu")
count(alldf)

# Drop all rows with NA
alldfNoNA <- dropna(alldf)
count(alldfNoNA)

# Create spark DF from R data.frame
dfSparkFaith <- createDataFrame(faithful)
createOrReplaceTempView(dfSparkFaith, "faith")

# Read Parquet
dfTaxi <- read.parquetarquet("wasb://datasets@datasetsnortheu.blob.core.windows.net/parquet/nyctaxitrip")
cache(dfTaxi)
count(dfTaxi)
showDF(dfTaxi, 10)

# Filter spark dataframe
dfLongTrips <- filter(dfTaxi, "trip_time_in_secs > 10000")
head(dfLongTrips)
count(dfLongTrips)

# Create HIVE Table
saveAsTable(df = dfLongTrips, tableName = "longTrips", mode = "overwrite")

# Read a file in blob storage to local data.frame (not recommended for large data)
dfIrisSpark = read.df("wasb://datasets@datasetsnortheu.blob.core.windows.net/csv/iris", "csv", header = "false", inferSchema = "true")
count(dfIrisSpark)
localDf = collect(dfIrisSpark)
class(localDf)

head(dfIrisSpark)
dfIrisSpark = createDataFrame(iris)
head(dfIrisSpark)
# run distributed R function on Spark DF
       
schemaOut <- structType(structField("Sepal_Length", "double"), 
                     structField("Sepal_Width", "double"), 
                     structField("Petal_Length", "double"),
                     structField("Petal_Width", "double"),
                     structField("Species", "string"),
                     structField("Sepal_x_10", "double"))
sparkDF <- dapply(dfIrisSpark, function(x) {
  # code
  x <- cbind(x, x$Sepal_Length * 10)
}, schemaOut)
rDF = collect(sparkDF)
rDF

# Collect directly to local R data.frame
dfIrisSpark = createDataFrame(iris)
dfRdapcol = dapplyCollect(dfIrisSpark, function(x) {
  # code
  # Add new col as sepal x 10
  x <- cbind(x, x$Sepal_Length * 10)
  # Rename col
  names(x)[names(x)=="x$Sepal_Length * 10"] <- "Sepal_x_10"
  x
})
dfRdapcol



