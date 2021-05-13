package hive_question_2

import org.apache.spark.sql.SparkSession

object hive_question_2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hive_question_2").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_2\\data")
    val header = rdd.first()
    //去掉表头
    val new_rdd = rdd.filter(_ != header)
      .map(line => Vedio(line.split("\t")(0).toLong, line.split("\t")(1).toLong, line.split("\t")(2).toLong)).toDF()
    new_rdd.printSchema()
    new_rdd.show(10)

    new_rdd.createOrReplaceTempView("Vedio")
    val sqlText = "select channl,count(1) visit_number,sum(min) totle_visit_times " +
      "from Vedio group by channl"
    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()
  }
}

case class Vedio(Uid: Long, channl: Long, min: Long)