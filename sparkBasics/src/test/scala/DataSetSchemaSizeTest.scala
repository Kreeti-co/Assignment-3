import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class DataSetSchemaSizeTest extends FunSuite{
  test("TestSpark.dataSet"){

    assert(TestSpark.dataSet()._1.schema.length===2) // for query1
    assert(TestSpark.dataSet()._2.schema.length===3) // for query2
    assert(TestSpark.dataSet()._3.schema.length===2) // for query3
  }



}
