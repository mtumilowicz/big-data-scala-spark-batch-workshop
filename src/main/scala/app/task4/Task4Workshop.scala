package app.task4

import app.task0.SparkWrapper
import org.apache.spark.sql.DataFrame

object Task4Workshop extends App with SparkWrapper {

  val address: DataFrame = loadJsonFile("task4/Dataset2")

  val customerInfo: DataFrame = loadCsvFile("task4/Dataset1.csv")

  val enrichedAddress = enrich(address, customerInfo)

  investigate(enrichedAddress)

  spark.stop()

  def enrich(data: DataFrame, additionalInfo: DataFrame): DataFrame = null
  // join data with additionalInfo on CustomerId, hint: data.join
}
