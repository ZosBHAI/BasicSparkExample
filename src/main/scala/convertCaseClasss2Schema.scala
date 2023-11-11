import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object convertCaseClasss2Schema extends App{

  val spark = SparkSession.builder().master("local[*]").appName("caseClass Conversion to Struct Type").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
    Row(Row("Michael ","Rose",""),"40288","M",4000),
    Row(Row("Robert ","","Williams"),"42114","M",4000),
    Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  val schema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("dob",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)
  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
 val rddDf = df.rdd
  rddDf.foreach(println)
  println("Printing the schema of the dataframe")
  df.printSchema()

  /*Creating the case class for the dataframe
   */

  case class name1 (first:String,last:String,middle:String)
  case class employee1 (fullName:name1,dob:String,gender:String,salary:Integer)

  /* Case class to StructType  */
  import spark.implicits._
  val caseClassSchema = Encoders.product[employee1].schema
  println(" Encoder class " + Encoders.product[employee1].clsTag)
  val newDF = spark.createDataFrame(rddDf,caseClassSchema)
  newDF.show()


  /*Creating Dataset from a RDD of row*/
  /*val datasetFromRdd = rddDf.map(x => {
    val nameStr = x(0).toString.split(",")
    val personData = nameStr.map( name => name1(name(0).toString,name(1).toString,name(2).toString))
    employee1(personData.toString,x(1).toString,x(2).toString,x(3))
  })*/



}
