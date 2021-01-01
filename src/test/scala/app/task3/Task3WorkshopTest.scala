package app.task3

import app.SparkSessionTestWrapper
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Task3WorkshopTest extends org.scalatest.FunSuite with SparkSessionTestWrapper {

  test("countCustomersByStateUsingApi") {
    import spark.implicits._

    //    given
    val inputAddresses = Seq(
      (1, "NY"),
      (2, "California"),
      (3, "NY"),
      (4, "California"),
      (5, "Oregon")
    ).toDF("CustomerId", "State")

    //    when
    val countByState = Task3.countCustomersByStateUsingApi(inputAddresses)

    //    then
    val asArray = countByState.collect().sortBy(_.getAs[String]("State"))
    asArray(0).getAs[String]("State") shouldBe "California"
    asArray(0).getAs[Int]("CustomersNumber") shouldBe 2
    asArray(1).getAs[String]("State") shouldBe "NY"
    asArray(1).getAs[Int]("CustomersNumber") shouldBe 2
    asArray(2).getAs[String]("State") shouldBe "Oregon"
    asArray(2).getAs[Int]("CustomersNumber") shouldBe 1
  }

  test("countCustomersByStateUsingSql") {
    import spark.implicits._

    //    given
    val inputAddresses = Seq(
      (1, "NY"),
      (2, "California"),
      (3, "NY"),
      (4, "California"),
      (5, "Oregon")
    ).toDF("CustomerId", "State")

    //    when
    val countByState = Task3.countCustomersByStateUsingSql(inputAddresses)

    //    then
    val asArray = countByState.collect().sortBy(_.getAs[String]("State"))
    asArray.length shouldBe 3
    asArray(0).getAs[String]("State") shouldBe "California"
    asArray(0).getAs[Int]("CustomersNumber") shouldBe 2
    asArray(1).getAs[String]("State") shouldBe "NY"
    asArray(1).getAs[Int]("CustomersNumber") shouldBe 2
    asArray(2).getAs[String]("State") shouldBe "Oregon"
    asArray(2).getAs[Int]("CustomersNumber") shouldBe 1
  }
}
