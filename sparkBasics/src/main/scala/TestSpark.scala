import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.IntegerType

object TestSpark {
  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Files into SparkRDD")
    .getOrCreate()

  def dataframe():(DataFrame,DataFrame,DataFrame) ={


    val df = sparkSession.read.format("csv").option("header", "true").option("delimiter", ",").load("src/main/datasetFolder/IMDbmovies1.csv")


    //Report of no of English titles produced each year

    val df1=df.filter(line=> line.toSeq.contains("English")).select("*").sort("year").groupBy("year").count()


    //reports of titles with highest votes for each year

    val df2=df.withColumn("votes",df.col("votes").cast(IntegerType)).groupBy("year","title","votes").max("votes").select("title","year","votes")


    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val df3=df.filter("year>=2000 AND year<=2010").withColumn("votes",df.col("votes").cast((IntegerType))).groupBy("director").max("votes")

    (df1,df2,df3)

  }

  def dataSet():(DataFrame,DataFrame,DataFrame)={
    import sparkSession.implicits._
    val movieDS = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/datasetFolder/IMDbmovies1.csv")
      .as[Movies]


    //Report of no of English titles produced each year

    val englishTitle=movieDS.filter(line => line.language == "English").select("*").sort("year").groupBy("year").count()

    //reports of titles with highest votes for each year

    val maxVotes=movieDS.withColumn("votes",movieDS.col("votes").cast(IntegerType)).select("title","year" , "votes").groupBy("year","votes").max("votes")

    //report of director’s list  with highest votes (sum of title’s votes aggregated to count) for their titles in the year range 2000-2010

    val directorList=movieDS.filter("year>=2000 AND year<=2010").withColumn("votes",movieDS.col("votes").cast((IntegerType))).groupBy("director").max("votes")


    (englishTitle,maxVotes,directorList)
  }






















}
