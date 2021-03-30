package main

import org.apache.spark.sql.{SparkSession,DataFrame}

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


//    count_topN_by_dataframe(sc,df,day,courseType)
    count_topN_by_sql(sc,df,day,courseType)

  }

  // 统计某一天每个课程的访问次数
  def count_topN_by_dataframe(spark:SparkSession,df:DataFrame,day:String,courseType:String): Unit ={

    //导入隐式声明
    import spark.implicits._
    // 使用dataframe方法统计
    val dayTopDF = df.filter($"day"===day && $"courseType"===courseType)
      // 按天和课程id分组
      .groupBy("day","courseId")
//      按courseId聚合 并降序排列
      .agg(count("courseId").as("times")).orderBy("times")
    dayTopDF.show(false)
  }

  def count_topN_by_sql(spark: SparkSession,df: DataFrame,day:String,courseType:String): Unit ={

    //创建一个临时试图表
    // 注意字符串的格式化及sql语句的空格
    df.createOrReplaceTempView("clean_log")
    val sqlText = "select day,courseId,count(1) as times from clean_log "+
                  s"where day='$day' and courseType='$courseType' "+
                  "group by day,courseId "+
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

  }
}
