package hive_question_1

import org.apache.spark.sql.SparkSession


//编写sql实现每个用户截止到每月为止的最大单月访问次数和累计到该月的总访问次数
object hive_question_1 {
  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("hive_question_1").master("local").getOrCreate()
      import spark.sqlContext.implicits._
      val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_1\\data")
      val header = rdd.first()
      //去掉表头
      val new_rdd = rdd.filter(_ != header)
        .map(line => Visit(line.split(",")(0), line.split(",")(1), line.split(",")(2).toLong)).toDF()
      new_rdd.printSchema()
      new_rdd.show(10)

      //over：关键字，表示前面的函数是分析函数，不是普通的聚合函数
      new_rdd.createOrReplaceTempView("Visits")
      val data = spark.sql("select userid,month,visits," +
        "max(visits) over(distribute by userid sort by month) maxvisit," +
        "sum(visits) over(distribute by userid sort by month) totalvisit " +
        "from (select userid,month,sum(visits) visits from visits group by userid,month) t1")
      data.printSchema()
      data.show()
  }
}

case class Visit(userid: String, month: String, visits: Long)
