package hive_question_1_11

import org.apache.spark.sql.SparkSession



// 每个用户连续登陆的最大天数
//结果如下：
//uid cnt_days
//1 3
//2 2
//3 1
//4 3
object hive_question_1_11 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hive_question_1_11").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_1_11\\data")
    val header = rdd.first()
    val new_rdd = rdd.filter(_ != header)
      .map(line => line.split(","))
      .map(line => Login(line(0), line(1)))
      .toDF()
    new_rdd.printSchema()
    new_rdd.show(10)


    new_rdd.createOrReplaceTempView("Login")


    val sqlText = "select uid,max(login_number) from" +
      "(select uid,count(date) login_number from " +
      "(select uid,date_sub(date,rm) date from " +
      "(select uid,date," +
      "row_number() over(distribute by uid order by date) rm " +
      "from Login) tem " +
      ") tem2 " +
      "group by uid,date order by uid) " +
      "group by uid"

    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()

  }
}

case class Login(uid: String, date: String)

