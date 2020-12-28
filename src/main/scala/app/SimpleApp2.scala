package app

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleApp2 {
  def main(args: Array[String]) {
    val spark = bootstrapSpark()
    val schema = new StructType()
      .add("RecordNumber", IntegerType, nullable = true)
      .add("Zipcode", IntegerType, nullable = true)
      .add("ZipCodeType", StringType, nullable = true)
      .add("City", StringType, nullable = true)
      .add("State", StringType, nullable = true)
    val logFile = "Task2_xxx"
    val logData: DataFrame = spark.read.json(logFile)

    logData.show()

    spark.stop()
  }

  def bootstrapSpark(): SparkSession =
    SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()
}