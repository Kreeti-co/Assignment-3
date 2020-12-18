import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite


class DataframeSchemaSizeTest extends FunSuite{
  test("TestSpark.dataframe") {

    // checking for the schema size after the query performed

    assert(TestSpark.dataframe()._1.schema.length===2) //query1
    assert(TestSpark.dataframe()._2.schema.length===3) //query2
    assert(TestSpark.dataframe()._3.schema.length===2) //query3

  }


}
