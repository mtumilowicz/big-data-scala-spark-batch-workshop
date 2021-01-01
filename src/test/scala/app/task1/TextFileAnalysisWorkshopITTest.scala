package app.task1

import app.SparkSessionTestWrapper
import org.scalatest.Ignore
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

@Ignore
class TextFileAnalysisWorkshopITTest extends org.scalatest.FunSuite with SparkSessionTestWrapper {

  test("testLineContaining") {
    import spark.implicits._

    val dataset = Seq("a", "a", "a", "b").toDS()
    val fileAnalysis = new TextFileAnalysisWorkshop(dataset)
    val numAs2 = fileAnalysis.lineContaining("a").count()
    val numBs2 = fileAnalysis.lineContaining("b").count()
    val numCs2 = fileAnalysis.lineContaining("c").count()

    numAs2 shouldBe 3
    numBs2 shouldBe 1
    numCs2 shouldBe 0
  }

}
