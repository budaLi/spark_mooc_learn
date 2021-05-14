package hive_question_5

import org.apache.spark.sql.SparkSession

//编写sql语句实现每班前三名，分数一样并列，同时求出前三名按名次排序的一次的分差：
//编写sql实现，结果如下：
//结果数据：
//班级	 stu_no	score	rn rn1	rn_diff
//1901	1	    90	  1	  1	    90
//1901	2	    90	  2	  1	    0
//1901	3	    83	  3	  3	    -7
//1902	7	    99	  1	  1	    99
//1902	9	    87	  2	  2	    -12
//1902	8	    67	  3	  3	    -20
object hive_question_5 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hive_question_5").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_5\\data")
    val header = rdd.first()
    val new_rdd = rdd.filter(_ != header)
      .map(line => line.split("\t"))
      .map(line => Score(line(0), line(1), line(2).toLong))
      .toDF()

    // "select classes,stu_no,score," +
    //        "row_number() over(distribute by classes order by score) rn,"+
    //        "rank() over(distribute by classes order by score) rn1 "+
    //        "from Score"
    //结果如下：
    //+-------+------+-----+---+---+
    //|classes|stu_no|score| rn|rn1|
    //+-------+------+-----+---+---+
    //|   1901|     4|   60|  1|  1|
    //|   1901|     3|   83|  2|  2|
    //|   1901|     1|   90|  3|  3|
    //|   1901|     2|   90|  4|  3|
    //|   1902|     6|   23|  1|  1|
    //|   1902|     5|   66|  2|  2|
    //|   1902|     8|   67|  3|  3|
    //|   1902|     9|   87|  4|  4|
    //|   1902|     7|   99|  5|  5|
    //+-------+------+-----+---+---+

    //需要再取出top3 和分数差

    //distribute by 相当于聚合

    //nvl（value，default_value）
    //空字段赋值，若value为空，则赋值default_value；若value非空，则返回原本value值。default_value可以是数值、'字符串'，也可以是字段

    //lag（col，n，defaultValue）：查询字段col，当前行往前数第n行的数据，若为null显示默认值defaultValue；
    //ps：lag(col)：指默认每次取字段col当前行的上一行数据


    new_rdd.createOrReplaceTempView("Score")
    val sqlText = "select classes,stu_no,score,rn,rn1," +
      "score - nvl(lag(score) over(distribute by classes order by score desc),0) rn_diff from" +
      "(select classes,stu_no,score," +
      "row_number() over(distribute by classes order by score desc) rn," +
      "rank() over(distribute by classes order by score desc) rn1 " +
      "from Score)  t1  " +
      "where t1.rn<4"
    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()
  }
}

case class Score(stu_no: String, classes: String, score: Long)
