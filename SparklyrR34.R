library(sparklyr)
sc <- spark_connect(master = "yarn-client", spark_home = "/usr/hdp/current/spark2-client", app_name = "MHTestR34")

# Try
sc <- spark_connect(master = "yarn", app_name = "test", spark_home="/usr/hdp/current/spark2-client", config = list(
  `sparklyr.shell.spark.driver-memory`="10g",
  spark.sql.shuffle.partitions="5000",
  spark.driver.maxResultSize="5000",
  spark.dynamicAllocation.enabled="true"
))




irisLoc <- file.path("wasb://datasets@datasetsnortheu.blob.core.windows.net","csv/iris/iris.csv")

irisDF <- spark_read_csv(sc, path = irisLoc, name = "AirTableFromR", header = FALSE, delimiter = ",")

class(irisDF)

library(dplyr)
irisDF %>% head
irisDF <- irisDF %>% rename (Petal_Length = V1 , Petal_Width = V2, Sepal_Length = V3, Sepal_width = V4, Class = V5)
irisDF %>% head
irisDF %>% select(Class)
irisDF %>% count(Class) 
myCount <- irisDF %>% count(Sepal_Length, sort = TRUE) %>% top_n(20)
myCount %>% filter(Sepal_Length < 1.5)