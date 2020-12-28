package app

import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class TextFileAnalysisITTest extends org.scalatest.FunSuite with SparkSessionTestWrapper {

  test("testLineContaining") {
    import spark.implicits._

    val dataset = Seq("a", "a", "a", "b").toDS()
    val fileAnalysis2 = new TextFileAnalysis(dataset)
    val numAs2 = fileAnalysis2.lineContaining("a").count()
    val numBs2 = fileAnalysis2.lineContaining("b").count()
    val numCs2 = fileAnalysis2.lineContaining("c").count()

    numAs2 shouldBe 3
    numBs2 shouldBe 1
    numCs2 shouldBe 0

  }

}
