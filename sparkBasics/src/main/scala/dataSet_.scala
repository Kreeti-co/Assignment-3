/*** Assignment-3 part3 ***/

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType


class DataSet(){
  val sparkSession = SparkSession.builder()
    .appName("Dataset basics")
    .master("local")
    .getOrCreate()

  //working with csv file
  dataSet_.dataSetUsingCsv(sparkSession)

  //working with json file
  dataSet_.dataSetUsingJson(sparkSession)
}
object dataSet_ {
  def main(args: Array[String]): Unit = {

    val obj=new DataSet()


  }
  def dataSetUsingCsv(sparkSession: SparkSession):(DataFrame,DataFrame,DataFrame)={
    import sparkSession.implicits._
    val movieDS = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/datasetFolder/IMDbmovies1.csv")
      .as[Movies]


    //Report of no of English titles produced each year

    val englishTitle=movieDS.filter(line => line.language == "English").select("*").sort("year").groupBy("year").count()
    englishTitle.collect().foreach(println)

    //reports of titles with highest votes for each year

    val maxVotes=movieDS.withColumn("votes",movieDS.col("votes").cast(IntegerType)).select("title","year" , "votes").groupBy("year","votes").max("votes")
    maxVotes.collect().foreach(println)

    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val directorList=movieDS.filter("year>=2000 AND year<=2010").withColumn("votes",movieDS.col("votes").cast((IntegerType))).groupBy("director").max("votes")
    directorList.collect().foreach(println)


    (englishTitle,maxVotes,directorList)
  }

  def dataSetUsingJson(sparkSession: SparkSession):(DataFrame,DataFrame,DataFrame)={
    import sparkSession.implicits._
    val movieDS=sparkSession.read
      .json("src/main/datasetFolder/imdb_movies.json")
      .as[Movies1]

    //Report of no of English titles produced each year

    val englishTitle=movieDS.filter(line => line.language == "English").select("*").sort("year").groupBy("year").count()

    englishTitle.collect().foreach(println)

    //reports of titles with highest votes for each year

    val maxVotes=movieDS.withColumn("votes",movieDS.col("votes").cast(IntegerType)).select("title","year" , "votes").groupBy("year","votes").max("votes")
    maxVotes.collect().foreach(println)

    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val directorList=movieDS.filter("year>=2000 AND year<=2010").withColumn("votes",movieDS.col("votes").cast((IntegerType))).groupBy("director").max("votes")
    directorList.collect().foreach(println)


    (englishTitle,maxVotes,directorList)




  }

}