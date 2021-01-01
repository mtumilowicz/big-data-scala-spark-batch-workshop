package app.task3

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task3 extends App {

  implicit val spark: SparkSession = bootstrapSpark()

  def address = loadCsvFile("task3/Dataset.csv")

  investigate(countByStateUsingApi(address))
  investigate(countByStateUsingSql(address))

  spark.stop()

  def loadCsvFile(filePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.option("header", "true").csv(filePath)

  def countByStateUsingApi(addresses: DataFrame)(implicit spark: SparkSession): DataFrame =
    addresses.groupBy("State")
      .agg(count("CustomerId").alias("CustomersNumber"))

  def countByStateUsingSql(addresses: DataFrame)(implicit spark: SparkSession): DataFrame = {
    addresses.createOrReplaceTempView("people")
    spark.sql("SELECT State, COUNT(CustomerId) as CustomersNumber FROM people GROUP BY State")
  }

  def bootstrapSpark(): SparkSession = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def investigate(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    dataFrame.printSchema()
  }

}
