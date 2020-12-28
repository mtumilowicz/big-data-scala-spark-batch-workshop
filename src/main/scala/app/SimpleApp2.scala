package app

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleApp2 {
  def main(args: Array[String]) {
    val spark = bootstrapSpark()
    val schema = "string STRING, int INT, array ARRAY<INT>, dict STRUCT<key : STRING>"
    val schemaTyped = new StructType()
      .add("string", StringType)
      .add("int", IntegerType)
      .add("array", ArrayType(StringType))
      .add("dict", new StructType().add("key", StringType))
    val source = scala.io.Source.fromFile("schema.json")
    val lines = try source.getLines.mkString finally source.close()
    val schemaFromJson = DataType.fromJson(lines).asInstanceOf[StructType]
    val logFile = "Task2_xxx"
    var logData: DataFrame = spark.read.schema(schemaFromJson).json(logFile)
    logData = logData
      .withColumn("xyz", col("dict.key2"))
      .drop(col("dict"))

    logData.show()
    logData.printSchema()

    val file = "Task2.csv"
    val xxx: DataFrame = spark.read.csv(file)
    xxx.show()
    xxx.printSchema()

    spark.stop()
  }

  def bootstrapSpark(): SparkSession =
    SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()
}