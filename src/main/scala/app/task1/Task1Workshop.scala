package app.task1

import app.task0.SparkWrapper

object Task1Workshop extends App with SparkWrapper {

  val file = loadTxtFile("task1/Dataset.txt").cache()
  val fileAnalysis = new TextFileAnalysisWorkshop(file)
  val numAs = fileAnalysis.lineContaining("a").count()
  val numBs = fileAnalysis.lineContaining("b").count()
  val numCs = fileAnalysis.lineContaining("c").count()
  println(s"Lines with a: $numAs, Lines with b: $numBs, Lines with b: $numCs")
  spark.stop()
}
