package app.task3

import app.task0.SparkWrapper
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task3Workshop extends App with SparkWrapper {

  def address = loadCsvFile("task3/Dataset.csv")

  investigate(countCustomersByStateUsingApi(address))
  investigate(countCustomersByStateUsingSql(address))

  spark.stop()

  def countCustomersByStateUsingApi(addresses: DataFrame)(implicit spark: SparkSession): DataFrame = null
  // hint: groupBy, agg, count, alias

  def countCustomersByStateUsingSql(addresses: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // prepare view, hint: createOrReplaceTempView
    // execute sql, hint: spark.sql
    null
  }

}
