package app.task2

import app.SparkSessionTestWrapper
import app.task2.Task2.unify
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Task2Test extends org.scalatest.FunSuite with SparkSessionTestWrapper {

  test("dropDuplicatedEntriesForCustomerId") {
    //    given
    val csvSchema = "CustomerId STRING, Zipcode STRING, ZipcodeType STRING, State STRING, City STRING"
    val address = unify(
      "task2/Dataset2",
      "task2/Dataset2_schema.json",
      "task2/Dataset1.csv",
      csvSchema)

    //    expect
    address.count() shouldBe 4
  }
}
