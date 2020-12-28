package app

import org.apache.spark.sql.Dataset

class TextFileAnalysis(file: Dataset[String]) {
  def lineContaining(str: String): Dataset[String] = file.filter(_.contains(str))
}
