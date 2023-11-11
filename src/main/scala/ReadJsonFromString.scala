
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.SparkSession
object ReadJsonFromString extends App{

  val spark = SparkSession.builder().appName("textToJson").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val jsonDataDF = spark.read.option("inferschema","true").format("json").
    load("D:\\Spark_Scala\\data\\textasJSON\\inputJSONdata.txt")
  jsonDataDF.show()

 //Reading JSON as text File
 val dfFromText = spark.read.text("D:\\Spark_Scala\\data\\textasJSON\\inputJSONdata.txt")
  dfFromText.show()

  val schema = new StructType()
    .add("Zipcode", StringType, true)
    .add("ZipCodeType", StringType, true)
    .add("City", StringType, true)
    .add("State", StringType, true)

  //Explode the JSON to various columns
  println("Exploding the JSON data")
  val explodeDFdfFromText = dfFromText.withColumn("jsondata",from_json(col("value"),schema))
  explodeDFdfFromText.show()
  explodeDFdfFromText.printSchema()


  //displaying only the json data as columns
  val onlyJsondata = explodeDFdfFromText.select("jsondata.*")
  onlyJsondata.show()


}
