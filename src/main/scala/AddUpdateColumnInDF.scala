import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, split}

object AddUpdateColumnInDF extends App {
  val spark = SparkSession.builder.appName("Add Or Update Column in Dataframe").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  case class peopleDetails (name:String, address:String)
  val data = Seq(Row(Row("James ","","Smith"),"36636","M","3000"),
    Row(Row("Michael ","Rose",""),"40288","M","4000"),
    Row(Row("Robert ","","Williams"),"42114","M","4000"),
    Row(Row("Maria ","Anne","Jones"),"39192","F","4000"),
    Row(Row("Jen","Mary","Brown"),"","F","-1")
  )

  val struture = new StructType().add("name",new StructType().add("first",StringType).
    add("middle",StringType).add("last",StringType)

  ).add("id", StringType).
    add("Gender",StringType).
    add("sal", StringType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),struture)

  df.show()
  import spark.implicits._
  //change the column datatype
  val  columnChangedDF = df.withColumn("sal",col("sal").cast("Integer"))

  //Derive a new column
  val deriveNewColumnDF = df.withColumn("copiedColumn",col("sal")* -1)
  deriveNewColumnDF.show()

  //Adding new constant to the Column
 df.withColumn("Country",lit("India")).show()

  //Renamign the column
  df.withColumnRenamed("Gender","Sex").show()

  //drop the column
  df.drop("id").show()

  val columns = Seq("name","address")
  val dataNew = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
    ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))

  val dfFromData = dataNew.toDF(columns:_*)
  dfFromData.printSchema()
//Parse the column
  val newdfFromData = dfFromData.map(x => {
    val name = x.getAs[String](0).split(",")
    val  address =  x.getAs[String](1).split(",")
    (name(0),name(1),address(0),address(1),address(2),address(3))
  })


  val finalDF = newdfFromData.toDF("First Name","Last Name",
    "Address Line1","City","State","zipCode")
  finalDF.show()



 // val encoderSchema = Encoder.product[peopleDetails].schema
  //val dsFromData = dataNew.toDF().as[peopleDetails]
  /*val newdsFromData = dsFromData.map(x => {
    val firstName = peopleDetails(0)
  })*/

}
