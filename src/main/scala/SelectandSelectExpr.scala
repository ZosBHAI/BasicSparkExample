import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object SelectandSelectExpr extends App {
  val spark = SparkSession.builder.appName("CreateDataFrame").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  var flightDF = spark.read.option("inferSchem",true).option("header",true).format("csv").
    load("D:\\Spark_Scala\\data\\SDGuide\\flightData\\2016flightdata.txt")

  flightDF.show(10)

 //Add new flag column  which indicates   destiantion and source are same
  //import spark.implicits._
  flightDF.selectExpr("*","(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as WithinCountry").show(10)

  //to Display the same in select
  flightDF.select(expr("*"),expr("(DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME) as WithinCountry")).show(10)

//Adding new Columns to existing dataframe
 flightDF =  flightDF.withColumn("WithinCountry",expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME"))
  flightDF.show(10)
}
