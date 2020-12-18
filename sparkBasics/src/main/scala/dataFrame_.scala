/*** Assignment-3 part2 ***/

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType

class Dataframe(){

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Files into SparkRDD")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  //working with csv file
  dataFrame_.dataframeUsingCsv(sparkSession)

  //working with json file
  dataFrame_.dataframeUsingJson(sparkSession)

}




object dataFrame_ {
  def main(args: Array[String]): Unit = {

    val obj=new Dataframe()



  }
  def dataframeUsingCsv(sparkSession: SparkSession):(DataFrame,DataFrame,DataFrame)={
    val df = sparkSession.read.format("csv").option("header", "true").option("delimiter", ",").load("src/main/datasetFolder/IMDbmovies1.csv")


    //Report of no of English titles produced each year

    val df1=df.filter(line=> line.toSeq.contains("English")).select("*").sort("year").groupBy("year").count()
      df1.show()


    //reports of titles with highest votes for each year

    val df2=df.withColumn("votes",df.col("votes").cast(IntegerType)).groupBy("year","title","votes").max("votes").select("title","year","votes")
      df2.show()


    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val df3=df.filter("year>=2000 AND year<=2010").withColumn("votes",df.col("votes").cast((IntegerType))).groupBy("director").max("votes")
      df3.show()

    (df1,df2,df3)
  }
  def dataframeUsingJson(sparkSession: SparkSession):(DataFrame,DataFrame,DataFrame)={
    val jsonDf=sparkSession.read.json("src/main/datasetFolder/imdb_movies.json")

    //Report of no of English titles produced each year

    val jsonDf1=jsonDf.filter(line=> line.toSeq.contains("English")).select("*").sort("year").groupBy("year").count()
      jsonDf1.show()

    //reports of titles with highest votes for each year

    val jsonDf2=jsonDf.withColumn("votes",jsonDf.col("votes").cast(IntegerType)).groupBy("year","title","votes").max("votes").select("title","year","votes")
      jsonDf2.show()

    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val jsonDf3=jsonDf.filter("year>=2000 AND year<=2010").withColumn("votes",jsonDf.col("votes").cast((IntegerType))).groupBy("director").max("votes")
      jsonDf3.show()

    (jsonDf1,jsonDf2,jsonDf3)

  }












}
