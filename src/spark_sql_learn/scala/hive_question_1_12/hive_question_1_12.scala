package hive_question_1_12

import org.apache.spark.sql.SparkSession


//id	sex	chinese_s	math_s
//0	  0	    70	    50
//1	  0	    90	    70
//2	  1	    80	    90


//1、男女各自语文第一名（0:男，1:女）
//2、男生成绩语文大于80，女生数学成绩大于70
object hive_question_1_12 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hive_question_1_12").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_1_12\\data")
    val header = rdd.first()
    val new_rdd = rdd.filter(_ != header)
      .map(line => line.split("\t"))
      .map(line => Score(line(0), line(1), line(2).toLong, line(3).toLong))
      .toDF()
    new_rdd.printSchema()
    new_rdd.show(10)


    new_rdd.createOrReplaceTempView("Score")

    //男女各自语文第一名（0:男，1:女）
    val sqlText = "select id,sex,chinese_s from " +
      "(select id,sex,chinese_s," +
      "row_number() over(distribute by sex order by chinese_s desc) rm " +
      "from Score) where rm=1 "


    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()

    //2、男生成绩语文大于80，女生数学成绩大于70
    val sqlText2 = "select id,sex,chinese_s from score where sex=0 and chinese_s>80 union " +
      "select id,sex,chinese_s from score where sex=1 and chinese_s>70"
    val data2 = spark.sql(sqlText2)
    data2.printSchema()
    data2.show()

  }
}

case class Score(id: String, sex: String, chinese_s: Long, math_s: Long)