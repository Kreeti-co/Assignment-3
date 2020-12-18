/*** Assignment-3 part4 ***/

import org.apache.spark.sql.{SparkSession}

object dataFramedataset {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Files into SparkRDD")
      .getOrCreate()


    sparkSession.sparkContext.setLogLevel("ERROR")

    dataFrame(sparkSession)

    dataSet(sparkSession)

  }

  def dataFrame(sparkSession: SparkSession): Unit={

      val english=dataFrame_.dataframeUsingCsv(sparkSession)._1
      val vote=dataFrame_.dataframeUsingCsv(sparkSession)._2
      val director=dataFrame_.dataframeUsingCsv(sparkSession)._3

      val englishJson=dataFrame_.dataframeUsingJson(sparkSession)._1
      val voteJson=dataFrame_.dataframeUsingJson(sparkSession)._2
      val directorJson=dataFrame_.dataframeUsingJson(sparkSession)._3


    //Report of no of English titles produced each year

    // writing result into csv
    english.write.csv("src/main/query-files/english.csv")

    // writing result into json
    englishJson.write.json("src/main/query-files/english.json")

    //writing the result into a parquet file
    english.write.parquet("src/main/query-files/english.parquet")

    //reports of titles with highest votes for each year

    // writing result into csv
    vote.write.csv("src/main/query-files/Votes.csv")

    // writing result into json
    voteJson.write.json("src/main/query-files/Votes.json")

    //writing the result into a parquet file
    vote.withColumnRenamed("max(votes)","maxVotes").write.parquet("src/main/query-files/Votes.parquet")

    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    // writing result into csv
    director.write.csv("src/main/query-files/director.csv")

    // writing result into json
    directorJson.write.json("src/main/query-files/director.json")

    //writing the result into a parquet file
    director.withColumnRenamed("max(votes)","maxVotes").write.parquet("src/main/query-files/director.parquet")


  }

  def dataSet(sparkSession: SparkSession): Unit ={
    val englishTitle=dataSet_.dataSetUsingCsv(sparkSession)._1
    val maxVotes=dataSet_.dataSetUsingCsv(sparkSession)._2
    val directorList=dataSet_.dataSetUsingCsv(sparkSession)._3

    val jsonEng=dataSet_.dataSetUsingJson(sparkSession)._1
    val jsonVote=dataSet_.dataSetUsingJson(sparkSession)._2
    val jsonDirector=dataSet_.dataSetUsingJson(sparkSession)._3



    //Report of no of English titles produced each year

    // writing result into csv
    englishTitle.write.csv("src/main/query-files/englishTitle.csv")

    // writing result into json
    jsonEng.write.json("src/main/query-files/englishTitle.json")

    //writing the result into a parquet file
    englishTitle.write.parquet("src/main/query-files/englishTitle.parquet")

    //reports of titles with highest votes for each year

    // writing result into csv
    maxVotes.write.csv("src/main/query-files/maxVotes.csv")

    // writing result into json
    jsonVote.write.json("src/main/query-files/maxVotes.json")

    //writing the result into a parquet file
    maxVotes.withColumnRenamed("max(votes)","maxVotes").write.parquet("src/main/query-files/maxVotes.parquet")

    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    // writing result into csv
    directorList.write.csv("src/main/query-files/directorList.csv")

    // writing result into json
    jsonDirector.write.json("src/main/query-files/directorList.json")

    //writing the result into a parquet file
    directorList.withColumnRenamed("max(votes)","maxVotes").write.parquet("src/main/query-files/directorsList.parquet")



  }






}

