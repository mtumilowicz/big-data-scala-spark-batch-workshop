package app.task2

import app.task0.SparkWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Task2 extends App with SparkWrapper {

  val csvSchema = "CustomerId INT, Zipcode STRING, ZipcodeType STRING, State STRING, City STRING"
  investigate(unify(
    "task2/Dataset2",
    "task2/Dataset2_schema.json",
    "task2/Dataset1.csv",
    csvSchema)
    .toDF()
  )

  spark.stop()

  def unify(jsonPath: String, jsonSchemaPath: String, csvPath: String, csvSchema: String)
           (implicit spark: SparkSession): Dataset[Address] = {
    val rawAddressJson: DataFrame = loadJsonFile(filePath = jsonPath, schemaPath = jsonSchemaPath)
    val purifiedAddressFromJson = purifyRawAddressFromJson(rawAddressJson)

    val rawAddressCsv: DataFrame = loadCsvFile(filePath = csvPath, schema = csvSchema)
    val purifiedAddressFromCsv = purifyRawAddressFromCsv(rawAddressCsv)

    dropDuplicatedEntriesForCustomerId(purifiedAddressFromCsv, purifiedAddressFromJson)
  }

  def dropDuplicatedEntriesForCustomerId(purifiedAddressFromCsv: Dataset[Address],
                                         purifiedAddressFromJson: Dataset[Address]): Dataset[Address] =
    purifiedAddressFromCsv.unionByName(purifiedAddressFromJson)
      .dropDuplicates("CustomerId")

  def purifyRawAddressFromJson(dataFrame: DataFrame)(implicit spark: SparkSession): Dataset[Address] = {
    import spark.implicits._

    dataFrame
      .withColumn("Zipcode", col("Zipcode.code"))
      .as[Address]
  }

  def purifyRawAddressFromCsv(dataFrame: DataFrame)(implicit spark: SparkSession): Dataset[Address] = {
    import spark.implicits._

    dataFrame
      .drop("ZipcodeType")
      .as[Address]
  }

}
