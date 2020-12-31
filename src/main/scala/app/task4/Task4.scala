//package app.task4
//
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//object Task4 {
//  def main(args: Array[String]) {
//    implicit val spark: SparkSession = bootstrapSpark()
//    val address: DataFrame = loadJsonFile("Task4_2")
//
//    val customerInfo: DataFrame = loadCsvFile("Task4_1.csv")
//
//    val enrichedAddress = address.join(customerInfo.as('info), customerInfo("CustomerId") === address("CustomerId"))
//      .select()
//
//    investigate(enrichedAddress)
//
//    spark.stop()
//  }
//
//  def loadJsonFile(filePath: String)(implicit spark: SparkSession): DataFrame =
//    spark.read.json(filePath)
//
//  def loadCsvFile(filePath: String)(implicit spark: SparkSession): DataFrame =
//    spark.read.option("header", "true").csv(filePath)
//
//  def investigate(dataFrame: DataFrame): Unit = {
//    dataFrame.show()
//    dataFrame.printSchema()
//  }
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
//}
