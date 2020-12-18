import org.scalatest.FunSuite

class DataFrameDataTest extends FunSuite{
  test("TestSpark.dataframe"){


    // checking max vote equals tp 537 for the year 1906 or not
    assert(TestSpark.dataframe()._2.select("votes").where("year==1906").take(1)(0).toSeq===Array(537))

    // checking for year 1911 -> here max vote = 2019 but checking for the negative case

    assert(TestSpark.dataframe()._2.select("votes").where("year==1911").take(1)(0).toSeq!=Array(171))

  }
}
