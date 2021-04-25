package ml

import caseclass.Ratings
import conf.Appconf
import org.apache.spark.sql.DataFrame

object ModelTraing extends Appconf {

  //加载训练数据和测试数据
  val traingData: DataFrame = spark.read.format("parquet").load("/tmp/trainData")
  val testData: DataFrame = spark.read.format("parquet").load("/tmp/testData")

  //将数据转换成rating类型
  val trainDataRating = traingData.rdd.map(x => Ratings(x.getInt(0), x.getInt(1), x.getDouble(2), x.getInt(3)))

}
