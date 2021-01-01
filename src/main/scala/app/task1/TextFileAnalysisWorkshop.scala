package app.task1

import org.apache.spark.sql.Dataset

class TextFileAnalysisWorkshop(file: Dataset[String]) {
  def lineContaining(str: String): Dataset[String] = null // hint: filter contains
}
