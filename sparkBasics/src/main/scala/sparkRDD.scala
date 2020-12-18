/*** Assignment-3 part1 ***/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object sparkRDD {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")


    //working with csv file
    rddUsingCsv(sparkSession)


    //working with json file
    rddUsingJson(sparkSession)

  }


  def rddUsingCsv(sparkSession: SparkSession):Unit={

    val rddToDf = sparkSession.read
      .csv("src/main/datasetFolder/IMDbmovies1.csv")
      .toDF("imdb_title_id", "title", "original_title", "year", "date_published", "genre", "duration", "country", "language", "director", "writer", "production_company", "actors", "description", "avg_vote", "votes", "budget", "usa_gross_income", "worlwide_gross_income", "metascore", "reviews_from_users", "reviews_from_critics")

    val df=rddToDf

    //Report of no of English titles produced each year

    val df1=df.filter(line=> line.toSeq.contains("English")).select("*").sort("year").groupBy("year").count()


    //reports of titles with highest votes for each year

    val df2=df.withColumn("votes",df.col("votes").cast(IntegerType)).groupBy("year","title","votes").max("votes").select("title","year","votes").withColumnRenamed("max(Votes)","voters").repartition(1)


    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val df3=df.filter("year>=2000 AND year<=2010").withColumn("votes",df.col("votes").cast((IntegerType))).groupBy("director").max("votes").show()

  }
  def rddUsingJson(sparkSession: SparkSession):Unit= {

    val rddToDf = sparkSession.read.option("header","true")
      .json("src/main/datasetFolder/imdb_movies.json")
      .toDF("actors", "avg_vote", "budget", "country","date_published", "description", "director", "duration", "genre", "language", "metascore", "original_title", "production_company", "reviews_from_critics", "reviews_from_users", "title", "title_id","usa_gross_income", "votes", "world_wide_gross_income", "writer", "year")

    val jsonDf =rddToDf

    //Report of no of English titles produced each year

    val jsonDf1 = jsonDf.filter(line => line.toSeq.contains("English")).select("*").sort("year").groupBy("year").count().show()


    //reports of titles with highest votes for each year

    val jsonDf2 = jsonDf.withColumn("votes", jsonDf.col("votes").cast(IntegerType)).groupBy("year", "title", "votes").max("votes").select("title", "year", "votes").show()


    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val jsonDf3 = jsonDf.filter("year>=2000 AND year<=2010").withColumn("votes", jsonDf.col("votes").cast((IntegerType))).groupBy("director").max("votes").show()

  }
}