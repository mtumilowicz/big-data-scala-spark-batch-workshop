package app.task2

import app.SparkSessionTestWrapper
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Task2WorkshopTest extends org.scalatest.FunSuite with SparkSessionTestWrapper {

  test("dropDuplicatedEntriesForCustomerId") {
    //    given
    val csvSchema = "CustomerId INT, Zipcode STRING, ZipcodeType STRING, State STRING, City STRING"
    val address = Task2Workshop.unify(
      jsonPath = "task2/Dataset2",
      jsonSchemaPath = "task2/Dataset2_schema.json",
      csvPath = "task2/Dataset1.csv",
      csvSchema = csvSchema)

    //    expect
    val asArray: Array[Address] = address.collect()
      .sortBy(_.customerId)
    asArray.length shouldBe 4
    asArray(0).customerId shouldBe 2
    asArray(1).customerId shouldBe 3
    asArray(2).customerId shouldBe 10
    asArray(3).customerId shouldBe 11
  }
}
