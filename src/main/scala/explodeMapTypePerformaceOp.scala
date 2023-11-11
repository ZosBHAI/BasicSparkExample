
import org.apache.spark.sql.types.{ArrayType, MapType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object explodeMapTypePerformaceOp  extends App{


  val spark = SparkSession.builder.appName("CreateDataFrame").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")


  val arrayArrayData = Seq(
    Row(Map("James" -> List(List("Java","Scala","C++"),List("Spark","Java")))),
    Row(Map("Michael" -> List(List("Spark","Java","C++"),List("Spark","Java")))),
    Row(Map("Robert" ->List(List("CSharp","VB"),List("Spark","Python"))))
  )

  val arraySchema = new StructType().add("NameLanguage",MapType(StringType,ArrayType(ArrayType(StringType))))

  //create the dataframe
  val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData),arraySchema)
  df.show(false)
  df.printSchema()



}
