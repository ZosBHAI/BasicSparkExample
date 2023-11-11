import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{expr,explode,flatten,col}

object ArrayOfStructColumns extends App {

  val spark = SparkSession.builder.appName("CreateDataFrame").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val arrayStructData = Seq(
    Row("James",List(Row("Java","XX",120),Row("Scala","XA",300))),
    Row("Michael",List(Row("Java","XY",200),Row("Scala","XB",500))),
    Row("Robert",List(Row("Java","XZ",400),Row("Scala","XC",250))),
    Row("Washington",null)
      )


  val arrayStructSchema = new StructType().add("name",StringType).
    add("booksIntersted",ArrayType(new StructType().
      add("name",StringType).
      add("author",StringType).
      add("pages",IntegerType)))


  val df = spark.createDataFrame(spark.sparkContext
    .parallelize(arrayStructData),arrayStructSchema)
  df.printSchema()
  df.show()

  ///converting all the list entries into row of a  single column
  val booksInterstedDF = df.select(explode(expr("booksIntersted")).as("bookDetails"))
  booksInterstedDF.show()

  booksInterstedDF.printSchema()
 //Convert abbove dataframe to 2  3 columns; Tis is easy because the it is of rowType

  val booksInterstedDFtoCol = booksInterstedDF.select("bookDetails.name","bookDetails.author","bookDetails.pages")
  booksInterstedDFtoCol.show()

  //Accessing List of List

  val arrayArrayData = Seq(
    Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
    Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
    Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
  )


  println("Converting the Array of Array to Columns")
  val arrayArraySchema = new StructType().add("name",StringType).
    add("subjects",ArrayType(ArrayType(StringType)))

  val arrayArrayDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
  arrayArrayDF.printSchema()
  arrayArrayDF.show(10)

  arrayArrayDF.select(
    arrayArrayDF("name") +: (0 until 2).map(i => arrayArrayDF("subjects")(i).alias(s"LanguagesKnown$i")): _*
  ).show()

  arrayArrayDF.select(expr("name"),explode(expr("subjects")).as("subjects")).show()

  //converting the nested array to single flat array
  println("flattening the nested Array")

  arrayArrayDF.select(col("name"),flatten(expr("subjects")).as("subjects")).show()
}
