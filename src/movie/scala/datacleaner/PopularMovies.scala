package datacleaner

import conf.Appconf
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode


//根据评分数据统计观看次数最多的topN电影的MovieId和title
object PopularMovies {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("PopularMovies").getOrCreate()
    //加载rating数据
    val ratingDataDf = spark.read.format("parquet").load("/tmp/rating")
    //电影数据
    val movieDataDf = spark.read.format("parquet").load("/tmp/movies")
    ratingDataDf.printSchema()
    ratingDataDf.show(10)

    val topNumber = 5
    ratingDataDf.createOrReplaceTempView("ratingData")
    movieDataDf.createOrReplaceTempView("movieData")
    //      val sqlText = s"select ratingData.movieId,count(*) as c,title from ratingData,movieData where ratingData.movieId=movieData.movieId   group by movieId ,order by c desc limit $topNumber"
    val sqlText = "select ratingData.movieId, count(*) as c, title from  ratingData join   movieData on ratingData.movieId = movieData.movieId group by ratingData.movieId,title order by c desc limit 5"
    val topN = spark.sql(sqlText)
    topN.show(truncate = false)
    topN.write.mode(SaveMode.Overwrite).parquet(s"/tmp/top$topNumber")

  }

}