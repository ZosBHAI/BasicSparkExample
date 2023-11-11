import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{concat,concat_ws}

object splitJoinColumns extends App{
  val spark = SparkSession.builder.appName("CreateDataFrame").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val data = Seq(("James, A, Smith","2018","M",3000),
    ("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),
    ("Maria,Anne,Jones","2005","F",4000),
    ("Jen,Mary,Brown","2010","",-1)
  )
  import spark.implicits._
  val df = data.toDF("name","dob_year","gender","salary")
  df.show()

  //Spliting the column
  val splitDF = df.withColumn("firstName",split(col("name"),",").getItem(0)).
    withColumn("middleName",split(col("name"),",").getItem(1)).
    withColumn("lastName",split(col("name"),",").getItem(2))


  splitDF.show()

  //select a column by splitting a particular column
  val splitDFsel = df.select(split(col("name"),",").getItem(0).as("firstName"),
    split(col("name"),",").getItem(1).as("middleName"),
    split(col("name"),",").getItem(2).as("lastName"))

  splitDFsel.show()

  //Concatenate

  println("concatenate")
  val dataCon = Seq(("James","A","Smith","2018","M",3000),
    ("Michael","Rose","Jones","2010","M",4000),
    ("Robert","K","Williams","2010","M",4000),
    ("Maria","Anne","Jones","2005","F",4000),
    ("Jen","Mary","Brown","2010","",-1)
  )

  val columns = Seq("fname","mname","lname","dob_year","gender","salary")
  //import spark.sqlContext.implicits._
  var dfCon = dataCon.toDF(columns:_*)
  dfCon.show(false)


  val seldfCon = dfCon.select(concat(col("fname"),lit(","),col("mname"),lit(","),
    col("lname")).as("name"))
  seldfCon.show()

  //using concate_ws function
  dfCon = dfCon.withColumn("name",concat_ws(",",col("fname"),col("mname"),col("lname")))
  dfCon.show()



}
