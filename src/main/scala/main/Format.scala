package main

import org.apache.spark.sql.SparkSession

//创建基于scala的maven项目要注意：
//1. 创建项目时需要选择scala
//2 pom文件要注意
//3 遇到偶尔的异常使用 mvn clean 即idea右侧的选项
//用spark解析日志文件
object Format {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("format").getOrCreate()
    var rdd = spark.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\main\\resources\\init.log")
    rdd.take(10).foreach(println)

  }
}
