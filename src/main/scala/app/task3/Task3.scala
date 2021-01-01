package app.task3

import app.task0.SparkWrapper
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task3 extends App with SparkWrapper {

  def address = loadCsvFile("task3/Dataset.csv")

  investigate(countByStateUsingApi(address))
  investigate(countByStateUsingSql(address))

  spark.stop()

  def countByStateUsingApi(addresses: DataFrame)(implicit spark: SparkSession): DataFrame =
    addresses.groupBy("State")
      .agg(count("CustomerId").alias("CustomersNumber"))

  def countByStateUsingSql(addresses: DataFrame)(implicit spark: SparkSession): DataFrame = {
    addresses.createOrReplaceTempView("people")
    spark.sql("SELECT State, COUNT(CustomerId) as CustomersNumber FROM people GROUP BY State")
  }

}
