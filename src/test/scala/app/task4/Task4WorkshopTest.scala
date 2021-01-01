package app.task4

import app.SparkSessionTestWrapper
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Task4WorkshopTest extends org.scalatest.FunSuite with SparkSessionTestWrapper {

  test("testEnrich") {
    import spark.implicits._

    //    given
    val address = Seq(
      (1, "NY"),
      (2, "California"),
      (3, "NY"),
    ).toDF("CustomerId", "State")

    val customerInfo = Seq(
      (1, "Miki"),
      (2, "Akseli")
    ).toDF("CustomerId", "CustomerName")

    //    when
    val asArray = Task4Workshop.enrich(address, customerInfo).collect()
      .sortBy(_.getAs[Int]("CustomerId"))

    //    then
    asArray.length shouldBe 2
    asArray(0).getAs[String]("CustomerId") shouldBe 1
    asArray(0).getAs[Int]("CustomerName") shouldBe "Miki"
    asArray(1).getAs[String]("CustomerId") shouldBe 2
    asArray(1).getAs[Int]("CustomerName") shouldBe "Akseli"
  }

}
