import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object ParqueReadWrite extends App {

  //For reading and writing Avro format , dependency is needed

  val spark = SparkSession.builder().appName("textToJson").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val csvDF = spark.read.format("csv").option("header","true").option("inferschema","true").
    load("D:\\Spark_Scala\\data\\JDBCsample\\school.csv")
  csvDF.show()

  //write this as Parquet file
  csvDF.write.mode(SaveMode.Overwrite).partitionBy("Nature_of_payment").parquet("D:\\Spark_Scala\\data\\parquestInput")

 //Reading from Partition using where
  println("Reading from Parquet Partitions")
  val parquetDF = spark.read.format("parquet").option("inferschema","true").option("header","true").
    load("D:\\Spark_Scala\\data\\parquestInput\\Nature_of_payment=Consulting%20Fee",
      "D:\\Spark_Scala\\data\\parquestInput\\Nature_of_payment=Gift")

  parquetDF.show()

  //Writing the parquet file as avro
  parquetDF.write.mode(SaveMode.Overwrite).format("avro").partitionBy("payer").save("D:\\Spark_Scala\\data\\avroInput")

  val avroDF = spark.read.format("avro").option("inferschema","true").option("header","true").
    load("D:\\Spark_Scala\\data\\avroInput").where(col("payer")=== "Southern Anesthesia & Surgical, Inc")
  avroDF.show()


}
