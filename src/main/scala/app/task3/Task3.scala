package app.task3

import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task3 extends App {

  implicit val spark: SparkSession = bootstrapSpark()

  def address = loadCsvFile("task3/Dataset.csv")

  investigate(aggregateUsingApi(address))
  investigate(aggregateUsingSql(address))

  spark.stop()

  def aggregateUsingApi(addresses: DataFrame)(implicit spark: SparkSession): DataFrame =
    addresses.groupBy(col("State"))
      .agg(count("CustomerId").alias("Customers"))

  def aggregateUsingSql(addresses: DataFrame)(implicit spark: SparkSession): DataFrame = {
    addresses.createOrReplaceTempView("people")
    spark.sql("SELECT State, COUNT(CustomerId) as Customers FROM people GROUP BY State")
  }

  def loadCsvFile(filePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.option("header", "true").csv(filePath)

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
