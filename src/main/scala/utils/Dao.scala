package utils

import java.sql.{Connection, PreparedStatement}

import caseclass.{DayTop, TrafficTop}

import scala.collection.mutable.ListBuffer

object Dao {
    //将topN的数据插入数据库
    //指定listBuffer时要指定数据格式 比如caseclass
  def insertDayTop(list:ListBuffer[DayTop]): Unit ={
      //var修饰的变量引用可改变 val修饰的变量引用不可改变
      var conn:Connection = null
      var state:PreparedStatement = null

      //下面这种提交sql的方式需要记忆  先建立连接，
      try {
        conn = MysqlUtil.getConnection()
        //关闭自动提交
        conn.setAutoCommit(false)
        val sql = "insert into day_top(day,courseId,times) values(?,?,?)"
        state = conn.prepareStatement(sql)
        for(ele <- list){
          state.setString(1,ele.day)
          state.setLong(2,ele.courseId)
          state.setLong(3,ele.times)

          //添加到一个batch中
          state.addBatch()
        }

        state.executeBatch()
        conn.commit()
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        //无论是否报错要关闭数据库
        MysqlUtil.release(conn, state)
      }

    }

  //topN 的traffic 入库
  def insert_traffic_topN(list: ListBuffer[TrafficTop]): Unit = {

    //建表  create table traffic_top(day Varchar(20) not null ,courseId Int not null ,traffic int not null , primary key (day,courseId) )
    var conn: Connection = null
    var state: PreparedStatement = null
    try {
      conn = MysqlUtil.getConnection()
      conn.setAutoCommit(false)
      val sql = "insert into traffic_top(day,courseId,traffic) values(?,?,?)"
      state = conn.prepareStatement(sql)
      for (ele <- list) {
        state.setString(1, ele.day)
        state.setLong(2, ele.courseId)
        state.setLong(3, ele.traffic)
        //添加到一个batch中
        state.addBatch()
      }

      state.executeBatch()
      conn.commit()
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      //无论是否报错要关闭数据库
      MysqlUtil.release(conn, state)
    }
  }
}
