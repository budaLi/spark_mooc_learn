package main

import caseclass.DayTop
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Dao

import scala.collection.mutable.ListBuffer

//使用agg等函数需要导入
import org.apache.spark.sql.functions._

// 统计多个topN
object TopN {

  def main(args: Array[String]): Unit = {
      //通过config 参数可以设置不启用 spark自带的类型判断功能
      val sc = SparkSession.builder().master("local[2]").appName("topN").
        config("spark.sql.sources.partitionColumnTypeInference.enabled", false).getOrCreate()
    val df = sc.read.format("parquet").load("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\main\\resources\\clear")
    df.printSchema()
    df.show(false)

    val day = "2017-05-11"
    val courseType = "article"


    //    count_topN_times_by_dataframe(sc,df,day,courseType)
    //      count_topN_times_by_sql(sc,df,day,courseType)
    count_topN_traffic_by_dataframe(sc, df, day, courseType)
    //      count_topN_traffic_by_sql(sc,df,day,courseType)

  }

  // 统计某一天每个课程的访问次数
  def count_topN_times_by_dataframe(spark: SparkSession, df: DataFrame, day: String, courseType: String): Unit = {

    //导入隐式声明
    import spark.implicits._
    // 使用dataframe方法统计
    val dayTopDF = df.filter($"day" === day && $"courseType" === courseType)
      // 按天和课程id分组
      .groupBy("day", "courseId")
      //      按courseId聚合 并降序排列
      .agg(count("courseId").as("times")).orderBy("times")
    dayTopDF.show(false)
  }

  def count_topN_times_by_sql(spark: SparkSession, df: DataFrame, day: String, courseType: String): Unit = {

    //创建一个临时试图表
    // 注意字符串的格式化及sql语句的空格
    df.createOrReplaceTempView("clean_log")
    val sqlText = "select day,courseId,count(1) as times from clean_log " +
      s"where day='$day' and courseType='$courseType' " +
      "group by day,courseId " +
      "order by times desc"

    val sqlText2 = "select day,courseId,count(1) as times " +
      "from clean_log " +
      "where day='2017-05-11' and courseType='article' " +
      "group by day,courseId " +
      "order by times desc"

    println(sqlText)
    println(sqlText2)
    val dayTopDF = spark.sql(sqlText)
    dayTopDF.show(false)

    // 将topN的数据插入数据库
    try {
      dayTopDF.foreachPartition(partition=>{
        //准备dayTop类型的listBuffer
        val list = new ListBuffer[DayTop]
        //对于每个partition的数据要放入listBuffer
        //对于partition的每个record都要获取其数据并放入listBuffer
        partition.foreach(record=>{
          val day = record.getAs[String]("day")
          val courseId = record.getAs[Long]("courseId")
          val times = record.getAs[Long]("times")

          //将取出来的数据通过caseclass封装后加入listBuffer
          list.append(DayTop(day,courseId,times))
        })

        //通过封装好的工具包存入数据库
        Dao.insertDayTop(list)
      }
      )
    }
    catch {
      case e: Exception => e.printStackTrace()
    }

  }

  //统计某天某种类型课程的流量topN 课程ID dataframe方式
  def count_topN_traffic_by_dataframe(spark: SparkSession, df: DataFrame, day: String, courseType: String): Unit = {
    //按dataframe统计 按day和courseType 过滤出数据 在通过courseId分组  将流量聚合 降序排列
    //聚合后的as重命名括号位置要注意
    import spark.implicits._
    val topN_traffic = df.filter($"day" === day && $"courseType" === courseType).
      groupBy("day", "courseId").agg(sum("traffic").as("totle_traffic")).orderBy("totle_traffic")

    topN_traffic.show(10)

    //入库处理
  }


  //统计某天某种类型课程的流量topN 课程ID
  def count_topN_traffic_by_sql(spark: SparkSession, df: DataFrame, day: String, courseType: String): Unit = {
    //sql查询方式  建立临时表视图 定义sql spark 提交sql
    df.createOrReplaceTempView("topN_traffic")

    //写sql的时候要注意 1.from 表名  2.group by时不用加括号  3. 各单词间有空格
    val sql_text = "select day,courseId,sum(traffic) as totle_traffic from topN_traffic " +
      s"where day='$day' and courseType='$courseType' " +
      "group by day,courseId  " +
      "order by totle_traffic desc"
    val topN_traffic = spark.sql(sql_text)
    topN_traffic.show(10)
  }
}
