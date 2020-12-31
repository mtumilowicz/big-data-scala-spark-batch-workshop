package app.task2

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task2 extends App {

  implicit val spark: SparkSession = bootstrapSpark()

  import spark.implicits._

  val rawAddressJson: DataFrame = loadJsonFile(filePath = "task2/Dataset2", schemaPath = "task2/Dataset2_schema.json")
  val purifiedAddressFromJson = rawAddressJson
    .withColumn("Zipcode", col("Zipcode.code"))
    .as[Address]

  investigate(rawAddressJson)
  investigate(purifiedAddressFromJson.toDF())

  val csvSchema = "CustomerId STRING, Zipcode STRING, ZipcodeType STRING, State STRING, City STRING"
  val rawAddressCsv: DataFrame = loadCsvFile(filePath = "task2/Dataset1.csv", schema = csvSchema)
  val purifiedAddressFromCsv = rawAddressCsv
    .drop(col("ZipcodeType"))
    .as[Address]

  investigate(rawAddressCsv)
  investigate(purifiedAddressFromCsv.toDF())

  val rawAddress = purifiedAddressFromCsv.unionByName(purifiedAddressFromJson)
  val uniqueAddress = rawAddress.dropDuplicates("CustomerId")

  investigate(uniqueAddress.toDF())

  spark.stop()

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
