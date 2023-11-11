import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{StringType,IntegerType}

object DataframeCreation  extends  App{

  val spark = SparkSession.builder.appName("CreateDataFrame").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val rddData = spark.sparkContext.parallelize(data)
  //To dataframe
  val dfFromRDD1 = rddData.toDF("language","users_count")
  dfFromRDD1.show()
 // Creating the dataframe  from schema  to row and dataframe
  //Defining the Schema -Programmatically Specifying the Schem
   val schema = new StructType()
     .add("language",StringType,true)
     .add("users_count",IntegerType,true)

  val rowrddData = rddData.map(x => Row(x._1.toString,x._2.toInt))
  val  rowrddDataDF = spark.createDataFrame(rowrddData,schema)
  rowrddDataDF.show()
  rowrddDataDF.printSchema()


  // Creating the Dataframme from schema
  //defining the schema - Case Class
  case class DataStructure(language:String,countValue:Int)
  val newrowrddDataDS = rddData.map(x => DataStructure(x._1,x._2.toInt))
   val newrowrddDataDF = newrowrddDataDS.toDF()
  println("Dataframe")
  newrowrddDataDF.show()
  println("Dataset")
  newrowrddDataDS.toDS().show()









}
