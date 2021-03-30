package utils

import utils.IpUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

// 将日志信息转换成所需要的格式
//输入：2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226
//输出：  http://www.imooc.com/article/17891  article  17891  407  218.75.35.226  北京  08:07:35  2017-05-11
object ConvertUtil {
    //定义一个存放数据的结构体
    var struct = StructType(
        Array(
          StructField("url",StringType),
          StructField("courseType",StringType),
          StructField("courseId",LongType),
          StructField("traffic",LongType),
          StructField("ip", StringType),
          StructField("city", StringType),
          StructField("time", StringType),
          StructField("day", StringType)

        )
    )

  def parseLong(log:String):Row={
      try {
        val splits = log.split("\t")
        val url = splits(1)
        val courseTypeId = url.replace("//","/").split("/")
        val courseType = courseTypeId(2)
        val courseId = courseTypeId(3).toLong
        val traffic = splits(2).toLong
        val ip = splits(3)
        val city = IpUtils.getCity(ip)
        val time = splits(0).split(" ")(1)
        val day = splits(0).split(" ")(0)
        Row(url, courseType, courseId, traffic, ip, city, time, day)
      }
    catch {
      case e:Exception=>println("异常",e)
        Row(0)
    }
  }

  def main(args: Array[String]): Unit = {
    println(parseLong("2017-05-11 08:07:35\thttp://www.imooc.com/article/17891\t407\t218.75.35.226"))
  }
}
