import org.apache.spark.sql.SparkSession

object BroadCasTExample extends App{
  val spark = SparkSession.builder.appName("CreateDataFrame").master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
  val countries = Map(("USA","United States of America"),("IN","India"))


  val broadcastStates = spark.sparkContext.broadcast(states)
  val broadcastCountries = spark.sparkContext.broadcast(countries)

  val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )
 val columns = Seq("firstname","lastname","country","state")
  import spark.implicits._
  val inputDF = data.toDF(columns:_*)
  inputDF.show()


  val outputDF = inputDF.map(row => {
    val inputDFCountry = row.getAs[String](2)
    val inputDFState = row.getString(3)

    val fullCountry = broadcastCountries.value.get(inputDFCountry).get
    val fullState = broadcastStates.value.get(inputDFState).get
    (row.getString(0),row.getString(1),fullCountry,fullState)
  }).toDF(columns:_*)


  outputDF.show(10)


}
