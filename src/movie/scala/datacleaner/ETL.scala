package datacleaner
import java.io.File
import caseclass.{Links, Movies, Ratings, Tags}
import org.apache.spark.sql.hive._
import org.apache.spark.sql.{SaveMode, SparkSession}

///清洗当前项目的所有数据 并存入Mysql数据库
object ETL {
  def main(args: Array[String]): Unit = {
    //通过enableHiveSupport 使用hive
    //    val warehouseLocation = "/user/hive/warehouse"
    //    val sc = SparkSession.builder().appName("ETL").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().master("local").getOrCreate()
    //    val sc = SparkSession.builder().appName("ETL").enableHiveSupport().master("local").getOrCreate()
    val sc = SparkSession.builder().appName("ETL").master("local").getOrCreate()

    //TODO 这里要注意导入sparkContext的隐式声明才可以toDF()
    import sc.sqlContext.implicits._

    //设置处理数据的最小分区
    val minPartitions = 2


    /**
     * 加载links数据
     */
    val links = sc.sparkContext.textFile("D:\\movie\\links.txt", minPartitions = minPartitions)
      //需要清洗数据 比如142 数据不全
      .filter(!_.endsWith(","))
      .map(line => line.split(","))
      .map(
        //        可通过  x.trim().toInt 改变其数据类型
        line => Links(line(0).trim().toInt, line(1).trim().toInt, line(2).trim().toInt))
      .toDF()

    links.show(10)

    /**
     * 加载movies数据
     */
    val movies = sc.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\movie\\resource\\movies.txt", minPartitions = minPartitions)
      .map(line => line.split(","))
      .map(
        //可通过  x.trim() 去除多余空格
        line => Movies(line(0).trim().toInt, line(1).trim(), line(2).trim()))
      .toDF()

    movies.show(10)


    /**
     * 加载加载rating数据数据 评分
     */
    val rating = sc.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\movie\\resource\\ratings.txt", minPartitions = minPartitions)
      .map(line => line.split(","))
      .map(
        //可通过  x.trim() 去除多余空格
        line => Ratings(line(0).trim().toInt, line(1).trim().toInt, line(2).trim().toDouble, line(3).trim().toInt))
      .toDF()

    rating.show(10)

    /**
     * 加载加载tags数据数据 评分
     */
    //tags 标签可能有多个逗号
    def rebuild(input: String): String = {
      val a = input.split(",")
      val head = a.take(2).mkString(",")
      val tail = a.takeRight(1).mkString
      val b = a.drop(2).dropRight(1).mkString.replace("\"", "")
      val output = head + "," + b + "," + tail
      output
    }

    val tags = sc.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\movie\\resource\\tags.txt", minPartitions = minPartitions)
      .filter(!_.endsWith(","))
      .map(x => rebuild(x))
      .map(line => line.split(","))
      .map(
        //可通过  x.trim() 去除多余空格
        line => Tags(line(0).trim().toInt, line(1).trim().toInt, line(2).trim(), line(3).trim().toInt))
      .toDF()

    tags.show(10)

    //将数据转换为parquet格式
    links.write.mode(SaveMode.Overwrite).parquet("D:/tmp/links")
    movies.write.mode(SaveMode.Overwrite).parquet("/tmp/movies")
    rating.write.mode(SaveMode.Overwrite).parquet("/tmp/rating")
    tags.write.mode(SaveMode.Overwrite).parquet("/tmp/tags")


    val df = sc.read.format("parquet").load("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\movie\\resource\\tmp\\links")
    df.show(10)


  }


}
