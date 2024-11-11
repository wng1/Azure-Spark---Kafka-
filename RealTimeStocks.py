%%configure -f
"conf": {
"spark. jars . packages" .
"spark. jars .excludes" .
2.11 : 2.2.0,
"org.scala-lang:scala-reflect,org.apache.spark:spark-tags 2.11"

##Set up Connection to Kafka

// Provide your Kafka broker endpoint (including port #)
val inputDf = (spark. readStream
  .format( "kafka" )
  .option( " kafka. bootstrap. servers " , "wne - kafka. mdlamldganads. gx. internal. cloudapp. net : 9092 " )
  .option("subscribe", "stockVa1s")
  .option("startingOffsets", "earliest")
  .load()

##Read Kafka into the Streaming dataframe

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val stockJsonDf = inputDf.selectExpr("CAST(value AS STRING)")
val stockSchema = (new StructType()
  .add("symbol", DataTypes.StringType)
  .add("time", DataTypes.StringType)
  .add("prices", DataTypes.FloatType)
  .add("size", DataTypes.IntegerType)
 )

val stockNestedJsonDf = stockJsonDf.select(from_json($"value", stockSchema).as("stockRecord"))
val stockFlatDf = stockNestedJsonDf.selectExpr("stockRecord.symbol", "stockRecord.time", "stockRecord.price", "stockRecord.size")
val stockDf = stockFlatDf.withColumn("time", from_unixtime($"time"/1000))

##Output Streaming Dataframe to Console
(stockDf.writeStream
  .outputMode("append")
  .format("console")
  .start()
  .awaitTermination(10000)
)

(stockDf.groupBy(
   window($"time", "4 seconds"),
   $"symbol"
   ).agg(max($"price"), min($"price"))
   .writeStream
   .format("console")
   .outputMode("complete")
   .start()
   .awaitTermination(30000)
)
   
