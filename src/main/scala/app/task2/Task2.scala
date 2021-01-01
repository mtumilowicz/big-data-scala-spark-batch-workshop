package app.task2

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Task2 extends App {

  implicit val spark: SparkSession = bootstrapSpark()

  val csvSchema = "CustomerId STRING, Zipcode STRING, ZipcodeType STRING, State STRING, City STRING"
  investigate(unify(
    "task2/Dataset2",
    "task2/Dataset2_schema.json",
    "task2/Dataset1.csv",
    csvSchema)
    .toDF()
  )

  spark.stop()

  def unify(jsonPath: String, jsonSchemaPath: String, csvPath: String, csvSchema: String)(implicit spark: SparkSession): Dataset[Address] = {
    val rawAddressJson: DataFrame = loadJsonFile(filePath = jsonPath, schemaPath = jsonSchemaPath)
    val purifiedAddressFromJson = purifyRawAddressFromJson(rawAddressJson)

    investigate(rawAddressJson)
    investigate(purifiedAddressFromJson.toDF())

    val rawAddressCsv: DataFrame = loadCsvFile(filePath = csvPath, schema = csvSchema)
    val purifiedAddressFromCsv = purifyRawAddressFromCsv(rawAddressCsv)

    investigate(rawAddressCsv)
    investigate(purifiedAddressFromCsv.toDF())

    dropDuplicatedEntriesForCustomerId(purifiedAddressFromCsv, purifiedAddressFromJson)
  }

  def dropDuplicatedEntriesForCustomerId(purifiedAddressFromCsv: Dataset[Address], purifiedAddressFromJson: Dataset[Address]) =
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

  def loadJsonFile(filePath: String, schemaPath: String)(implicit spark: SparkSession): DataFrame = {
    val source = scala.io.Source.fromFile(schemaPath)
    val lines = try source.getLines.mkString finally source.close()
    val schemaFromJson = DataType.fromJson(lines).asInstanceOf[StructType]
    spark.read.schema(schemaFromJson).json(filePath)
  }

  def loadCsvFile(filePath: String, schema: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.schema(schema).option("header", "true").csv(filePath)
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
