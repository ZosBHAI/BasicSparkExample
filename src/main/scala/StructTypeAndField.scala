import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, MapType, StringType, StructType}

object StructTypeAndField  extends App{

  val spark = SparkSession.builder.appName("StructTypeExample").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  /*
  Data definition
   */

  val simpleData = Seq(Row("James ","","Smith","36636","M",3000),
    Row("Michael ","Rose","","40288","M",4000),
    Row("Robert ","","Williams","42114","M",4000),
    Row("Maria ","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

  val schema = new StructType().
    add("firstName",StringType,true).
    add("middleName",StringType,true).
    add("lastaname",StringType,true).
    add("id",StringType,true).
    add("gender", StringType, true).
    add("salary",IntegerType,true)

  val dataframeDF = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),schema)
  dataframeDF.show()


  // Creating the Nested dataframe

  val nestedData = Seq(
    Row(Row("James ","","Smith"),"36636","M",3100),
    Row(Row("Michael ","Rose",""),"40288","M",4300),
    Row(Row("Robert ","","Williams"),"42114","M",1400),
    Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  //Definining the Schema

  val nestedSchema = new StructType().add("name",new StructType().
    add("firstName",StringType,true).
    add("middleName",StringType,true).
    add("lastaname",StringType,true)
  ).add("id",StringType,true).add("gender",StringType,true).add("salary",IntegerType,true)

  val nesteddataframeDF = spark.createDataFrame(spark.sparkContext.parallelize(nestedData),nestedSchema)
  nesteddataframeDF.show()

 //*Reading from JSON that reperesent the schema

  val x = nestedSchema.prettyJson
  val schemaFromJson = DataType.fromJson(x).asInstanceOf[StructType]
  val df3 = spark.createDataFrame(
    spark.sparkContext.parallelize(nestedData),schemaFromJson)
  df3.printSchema()

//Using Maptype and Arraytype
val arrayStructureData = Seq(
  Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
  Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
  Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
  Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
  Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
)
  val arrayStructureDataschema = new StructType().add("name",new StructType().add("first",StringType).
    add("middle",StringType).
    add("last",StringType)).add("Hobbies",ArrayType(StringType)).
    add("Featues",MapType(StringType,StringType))

  val arrayStructureDataDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureDataschema)
  arrayStructureDataDF.show()
  arrayStructureDataDF.printSchema()
  //Converting the case class to spark StructType
 case class name1 (first:String , middle:String , last:String)
  case class details (nameDetails:name1,hobbies: List[String],features:Map[String, String])
  //import spark.implicits._
 //case class details (nameDetails:name1,id: String,gender:String, salary:Integer)
 // val nestSchema11 = ScalaReflection.schemaFor(name1).dataType.asInstanceOf[StructType]
  val encoderSchema = Encoders.product[details].schema
  encoderSchema.printTreeString()

  val encoderSchemaDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),encoderSchema)
  encoderSchemaDF.show()
}
