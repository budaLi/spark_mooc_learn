package hive_question_6

import org.apache.spark.sql.SparkSession


//每个店铺的当月销售额和累计到当月的总销售额
object hive_question_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hive_question_6").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_6\\data")
    val header = rdd.first()
    val new_rdd = rdd.filter(_ != header)
      .map(line => line.split(","))
      .map(line => Sales(line(0), line(1), line(2).toLong))
      .toDF()
    new_rdd.show()
    new_rdd.createOrReplaceTempView("Sales")

    val sqlText = "select name,month,sales," +
      "sum(sales) over(distribute by name sort by month) totalvisit_sales from " +
      " (select name,month,sum(sales) sales " +
      "from Sales group by name,month order by name,month) t1"
    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()
  }
}


case class Sales(name: String, month: String, sales: Long)