//package app.task2
//
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
//
//object Task2 {
//
//  def main(args: Array[String]) {
//    import spark.implicits._
//
//    implicit val spark: SparkSession = bootstrapSpark()
//    val rawAddressJson: DataFrame = loadJsonFile("Task2_2")
//    val purifiedAddressFromJson = rawAddressJson
//      .withColumn("Zipcode", col("Zipcode.code"))
//      .as[Address]
//
//    investigate(rawAddressJson)
//    investigate(purifiedAddressFromJson)
//
//    val rawAddressCsv: DataFrame = loadCsvFile("Task2_1.csv")
//    val purifiedAddressFromCsv = rawAddressCsv
//      .drop(col("ZipcodeType"))
//      .as[Address]
//
//    investigate(rawAddressJson)
//    investigate(purifiedAddressFromCsv)
//
//    val rawAddress = purifiedAddressFromCsv.unionByName(purifiedAddressFromJson)
//    val addresses = rawAddress.dropDuplicates("CustomerId")
//
//    investigate(addresses)
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
//  def investigate[T](dataset: Dataset[T]): Unit = {
//    dataset.show()
//    dataset.printSchema()
//  }
//
//}
