package app

import org.apache.spark.sql.{Dataset, SparkSession}

object SimpleApp {
  def main(args: Array[String]) {
    val spark = bootstrapSpark()
    val logFile = "Task1.txt"
    val logData: Dataset[String] = spark.read.textFile(logFile).cache()
    val fileAnalysis = new TextFileAnalysis(logData)
    val numAs = fileAnalysis.lineContaining("a").count()
    val numBs = fileAnalysis.lineContaining("b").count()
    val numCs = fileAnalysis.lineContaining("c").count()
    println(s"Lines with a: $numAs, Lines with b: $numBs, Lines with b: $numCs")
    spark.stop()
  }

  def bootstrapSpark(): SparkSession =
    SparkSession.builder
      .appName("Simple Application")
      .master("local")
      .getOrCreate()
}