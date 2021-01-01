package app.task2

import app.task0.SparkWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Task2Workshop extends App with SparkWrapper {

  val csvSchema = "CustomerId INT, Zipcode STRING, ZipcodeType STRING, State STRING, City STRING"
  investigate(unify(
    jsonPath = "task2/Dataset2",
    jsonSchemaPath = "task2/Dataset2_schema.json",
    csvPath = "task2/Dataset1.csv",
    csvSchema = csvSchema)
    .toDF()
  )

  spark.stop()

  def unify(jsonPath: String, jsonSchemaPath: String, csvPath: String, csvSchema: String)
           (implicit spark: SparkSession): Dataset[Address] = {
    // loadJsonFile
    // purify address, hint: purifyRawAddressFromJson
    // investigate raw vs purified, hint: investigate, toDF()
    // loadCsvFile
    // purify address, hint: purifyRawAddressFromCsv
    // investigate raw vs purified, hint: investigate, toDF()
    // drop duplicates by customerId, hint: dropDuplicatedEntriesForCustomerId
    null
  }

  def dropDuplicatedEntriesForCustomerId(purifiedAddressFromCsv: Dataset[Address],
                                          purifiedAddressFromJson: Dataset[Address]): Dataset[Address] = null
  // sum sets and drop duplicates, hint: unionByName, dropDuplicates

  def purifyRawAddressFromJson(dataFrame: DataFrame)(implicit spark: SparkSession): Dataset[Address] = {
    import spark.implicits._

    // replace struct Zipcode with column Zipcode.code, hint: withColumn
    // map to case class Address, hint: as[T]
    null
  }

  def purifyRawAddressFromCsv(dataFrame: DataFrame)(implicit spark: SparkSession): Dataset[Address] = {
    import spark.implicits._

    // drop ZipcodeType column, hint: drop
    null
  }

}
