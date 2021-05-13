package hive_question_3

import org.apache.spark.sql.SparkSession


//编写连续7天登录的总人数
object hive_question_3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hive_question_3").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_3\\data")
    val header = rdd.first()
    //去掉表头
    val new_rdd = rdd.filter(_ != header)
      .map(line => Login(line.split(" ")(0).toLong, line.split(" ")(1), line.split(" ")(2))).toDF()
    new_rdd.printSchema()
    new_rdd.show(10)

    //data_sub(start_data,days)  返回指定的start_date减去days天后的日期
    // 因为要求七天连续登陆 所以可以先用row_number()将每个用户成功登录的数据进行排序 （1,2,3,4），且每天只需成功登录一次即可，需要根据uid去重
    // select uid,dt,row_number()  over(distribute by uid order by dt) from login where login_status=1 t1
    // 返回值如下：
    //+---+----------+---+
    //|uid|        dt| rm|
    //+---+----------+---+
    //|  1|2019-07-11|  1|
    //|  1|2019-07-12|  2|
    //|  1|2019-07-13|  3|
    //|  1|2019-07-14|  4|
    //|  1|2019-07-15|  5|
    //|  1|2019-07-16|  6|
    //|  1|2019-07-17|  7|
    //|  1|2019-07-18|  8|
    //|  3|2019-07-11|  1|
    //|  3|2019-07-12|  2|
    //|  3|2019-07-13|  3|
    //|  3|2019-07-14|  4|
    //|  3|2019-07-15|  5|
    //|  3|2019-07-16|  6|
    //|  3|2019-07-17|  7|
    //|  3|2019-07-18|  8|
    //|  2|2019-07-11|  1|
    //|  2|2019-07-12|  2|
    //|  2|2019-07-14|  3|
    //|  2|2019-07-15|  4|
    //+---+----------+---+

    //由上图数据，使用date_sub() 可求出每个uid登录的重复次数，如下：
    //select t1.uid uid,date_sub(t1.dt,t1.rm) dt from (select uid,dt,row_number()  over(distribute by uid order by dt) rm  from login where login_status=1) t1
    //+---+----------+
    //|uid|        dt|
    //+---+----------+
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  1|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  3|2019-07-10|
    //|  2|2019-07-10|
    //|  2|2019-07-10|
    //|  2|2019-07-11|
    //|  2|2019-07-11|

    //由上表数据可由uid，dt统计每个人出现的次数 通过count()过滤小于7的人 sql如下不再列出  结果如下
    //+---+----------+
    //|uid|        dt|
    //+---+----------+
    //|  3|2019-07-10|
    //|  1|2019-07-10|
    //+---+----------+

    //其中比较重要的包括where和having的使用
    //having子句与where都是过滤语句。
    //where 子句的作用是在对查询结果进行分组前，将不符合where条件的行去掉，即在分组之前过滤数据，条件中不能包含聚合函数，使用where条件显示特定的行。
    //having 子句的作用是筛选满足条件的组，即在分组之后过滤数据，条件中经常包含聚合函数，使用having条件显示特定的组，也可以使用多个分组标准进行分组。和group by一样，having 子句中的每一个元素也必须出现在select列表中。
    //总之，select 用where过滤，找到符合条件的元组。而having 用在group by后，配合使用，过滤结果。
    new_rdd.createOrReplaceTempView("Login")
    val sqlText = "select uid,dt from " +
      "(select t1.uid uid,date_sub(t1.dt,t1.rm) dt from " +
      " (select uid,dt,row_number()  over(distribute by uid order by dt) rm  from login where login_status=1) t1) t2 " +
      "group by uid,dt having count(uid)>7"

    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()
  }
}

case class Login(Uid: Long, dt: String, login_status: String)