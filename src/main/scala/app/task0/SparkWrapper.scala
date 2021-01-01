package app.task0

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait SparkWrapper {

  implicit lazy val spark: SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("spark test example")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def loadTxtFile(filePath: String)(implicit spark: SparkSession): Dataset[String] =
    spark.read.textFile(filePath)

  def loadJsonFile(filePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.json(filePath)

  def loadJsonFile(filePath: String, schemaPath: String)(implicit spark: SparkSession): DataFrame = {
    val source = scala.io.Source.fromFile(schemaPath)
    val lines = try source.getLines.mkString finally source.close()
    val schemaFromJson = DataType.fromJson(lines).asInstanceOf[StructType]
    spark.read.schema(schemaFromJson).json(filePath)
  }

  def loadCsvFile(filePath: String, schema: String)(implicit spark: SparkSession): DataFrame =
    spark.read.schema(schema).option("header", "true").csv(filePath)

  def loadCsvFile(filePath: String)(implicit spark: SparkSession): DataFrame =
    spark.read.option("header", "true").csv(filePath)

  def investigate(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    dataFrame.printSchema()
  }

}