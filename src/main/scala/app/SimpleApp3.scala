package app

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleApp3 {
  def main(args: Array[String]) {
    val spark = bootstrapSpark()
    val source = scala.io.Source.fromFile("Task2_json_schema.json")
    val lines = try source.getLines.mkString finally source.close()
    val schemaFromJson = DataType.fromJson(lines).asInstanceOf[StructType]
    val logFile = "Task2_xxx"
    var logData: DataFrame = spark.read.schema(schemaFromJson).json(logFile)

    val file = "Task3_yyy.csv"
    var xxx: DataFrame = spark.read.option("header", "true").csv(file)

    logData.join(xxx.as('info), xxx("CustomerId") === logData("CustomerId"))
      .select("CustomerName", "State")
      .show()

    spark.stop()
  }

  def bootstrapSpark(): SparkSession = {
    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }
}