package hive_question_1_9


import org.apache.spark.sql.SparkSession


//t1表:
//order_id  order_type  order_time
//111		  N           10:00
//111       	  A           10:05
//111           B           10:10
//
//是用hql获取结果如下：
//order_id  order_type_1  order_type_2  order_time_1  order_time_2
//111       N             A              10:00           10:05
//111       A             B              10:05           10:10


object hive_question_1_9 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hive_question_1_9").master("local").getOrCreate()
    import spark.sqlContext.implicits._
    val rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\spark_sql_learn\\scala\\hive_question_1_9\\data")
    val header = rdd.first()
    val new_rdd = rdd.filter(_ != header)
      .map(line => line.split("\t"))
      .map(line => Order(line(0).toLong, line(1), line(2)))
      .toDF()
    new_rdd.printSchema()
    new_rdd.show(10)


    new_rdd.createOrReplaceTempView("Order")

    //需要按order_id 聚合
    val sqlText = "select order_id," +
      "lag(order_type) over(distribute by order_id sort by order_time) order_type_1," +
      "order_type order_type_2," +
      "lag(order_time) over(distribute by order_id sort by order_time) order_time_1," +
      "order_time order_time_2 " +
      "from Order having order_type_1 is not null"
    val data = spark.sql(sqlText)
    data.printSchema()
    data.show()

  }
}

case class Order(order_id: Long, order_type: String, order_time: String)
