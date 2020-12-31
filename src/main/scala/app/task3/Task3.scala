//package app.task3
//
//import org.apache.spark.sql.functions.{col, count}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//object Task3 {
//
//  def aggregateUsingApi(addresses: DataFrame)(implicit spark: SparkSession) = {
//    val customersCount = addresses.groupBy(col("State"))
//      .agg(count("CustomerId").alias("Customers"))
//
//    investigate(customersCount)
//  }
//
//  def aggregateUsingSql(addresses: DataFrame)(implicit spark: SparkSession): DataFrame = {
//    addresses.createOrReplaceTempView("people")
//    spark.sql("SELECT State, COUNT(CustomerId) as Customers FROM people GROUP BY State")
//  }
//
//  def main(args: Array[String]) {
//
//    implicit val spark: SparkSession = bootstrapSpark()
//
//    def addresses = loadCsvFile("Task3.csv")
//
//    aggregateUsingApi(addresses)
//    aggregateUsingSql(addresses)
//
//    spark.stop()
//  }
//
//  def loadCsvFile(filePath: String)(implicit spark: SparkSession): DataFrame =
//    spark.read.option("header", "true").csv(filePath)
//
//  def bootstrapSpark(): SparkSession = {
//    val spark = SparkSession.builder
//      .appName("Simple Application")
//      .master("local")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//
//    spark
//  }
//
//  def investigate(dataFrame: DataFrame): Unit = {
//    dataFrame.show()
//    dataFrame.printSchema()
//  }
//
//}
