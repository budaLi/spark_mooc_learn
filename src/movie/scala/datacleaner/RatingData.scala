package datacleaner

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


//将评分数据拆分为训练集和测试集 并写入hdfs
object RatingData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("RatingData").getOrCreate()

    //通过这种方式可直接将parquet文件读取为dataframe
    val ratingDataDf = spark.read.format("parquet").load("/tmp/rating")
    ratingDataDf.printSchema()
    ratingDataDf.show(10)

    //通过sql方式操作rating数据
    //将rating数据升序排列 取前n条数据做训练集
    get_train_data(ratingDataDf, spark, 10000)
    get_test_data(ratingDataDf, spark, 100)


    def get_train_data(df: DataFrame, spark: SparkSession, number: Long): Unit = {
      /*
         获取训练数据
       */
      ratingDataDf.createOrReplaceTempView("ratingDataDf")
      val sqlText = s"select userId,movieId,rating from ratingDataDf order by timestamp asc limit $number"
      val trainData = spark.sql(sqlText)
      trainData.write.mode(SaveMode.Overwrite).parquet("/tmp/trainData")
      trainData.show(10)
    }

    def get_test_data(df: DataFrame, spark: SparkSession, number: Long): Unit = {
      /*
         获取训练数据
       */
      ratingDataDf.createOrReplaceTempView("ratingDataDf")
      val sqlText = s"select userId,movieId,rating from ratingDataDf order by timestamp desc limit $number"
      val trainData = spark.sql(sqlText)
      trainData.write.mode(SaveMode.Overwrite).parquet("/tmp/testData")
      trainData.show(10)
    }

  }
}
