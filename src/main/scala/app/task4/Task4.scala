package app.task4

import org.apache.spark.sql.{DataFrame, SparkSession}

object Task4 extends App {

  implicit val spark: SparkSession = bootstrapSpark()

  val address: DataFrame = loadJsonFile("task4/Dataset2")

  val customerInfo: DataFrame = loadCsvFile("task4/Dataset1.csv")

  val enrichedAddress = enrich(address, customerInfo)

  investigate(enrichedAddress)

  spark.stop()

  def enrich(data: DataFrame, additionalInfo: DataFrame): DataFrame =
    data.join(additionalInfo, "CustomerId")

  def loadJsonFile(filePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.json(filePath)

  def loadCsvFile(filePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.option("header", "true").csv(filePath)

  def investigate(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    dataFrame.printSchema()
  }

  def bootstrapSpark(): SparkSession = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }
}
