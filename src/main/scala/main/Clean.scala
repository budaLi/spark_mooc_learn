package main

import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.ConvertUtil

//将清洗后的日志信息保存到文件
object Clean {
  def main(args: Array[String]): Unit = {

      val sc = SparkSession.builder().master("local[2]").appName("CleanJob").getOrCreate()
      val rdd = sc.sparkContext.textFile("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\main\\resources\\Format")


      // 用封装好的转换方法和定义的好的结构体 解析文件的每一行数据 要注意选择createDataFrame函数时参数不同
      val df = sc.createDataFrame(rdd.map(line=>ConvertUtil.parseLong(line)),ConvertUtil.struct)
      df.printSchema()

      //truncate 截断
//      df.show(truncate = false)
    df.show(10)
      //coalesce 合并
      df.coalesce(numPartitions = 1).
      //按天将日志分区 parquet是spark sql默认的存储格式 列式存储
        write.format("parquet").partitionBy("day").
        mode(SaveMode.Overwrite).save("E:\\桌面\\工作算法\\spark_mooc_learn\\src\\main\\resources\\clear")


      sc.stop()
  }
}
