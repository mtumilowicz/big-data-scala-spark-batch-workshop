package app.task1

import org.apache.spark.sql.{Dataset, SparkSession}

object Task1 extends App {

  implicit val spark: SparkSession = bootstrapSpark()
  val file = loadFile("Dataset.txt").cache()
  val fileAnalysis = new TextFileAnalysis(file)
  val numAs = fileAnalysis.lineContaining("a").count()
  val numBs = fileAnalysis.lineContaining("b").count()
  val numCs = fileAnalysis.lineContaining("c").count()
  println(s"Lines with a: $numAs, Lines with b: $numBs, Lines with b: $numCs")
  spark.stop()

  def loadFile(filePath: String)(implicit spark: SparkSession): Dataset[String] =
    spark.read.textFile(filePath)

  def bootstrapSpark(): SparkSession =
    SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()
}
