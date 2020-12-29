package app

import org.apache.spark.sql.functions.{col, count}
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
    val source = scala.io.Source.fromFile("Task2_json_schema.json")
    val lines = try source.getLines.mkString finally source.close()
    val schemaFromJson = DataType.fromJson(lines).asInstanceOf[StructType]
    val logFile = "Task2_xxx"
    var logData: DataFrame = spark.read.schema(schemaFromJson).json(logFile)
    logData = logData
      .withColumn("Zipcode", col("Zipcode.code"))

    logData.show()
    logData.printSchema()

    val file = "Task2.csv"
    var xxx: DataFrame = spark.read.option("header", "true").csv(file)
    xxx = xxx.drop(col("ZipcodeType"))
    xxx.show()
    xxx.printSchema()

    var union = xxx.unionByName(logData)
    union = union.dropDuplicates("CustomerId")
    union.show()

    val agg = union.groupBy(col("State"))
      .agg(count("CustomerId").alias("Customers"))

    agg.show()

    union.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT State, COUNT(CustomerId) as Customers FROM people GROUP BY State")
    sqlDF.show()

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