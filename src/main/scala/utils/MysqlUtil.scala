package utils

import java.sql.{Connection, DriverManager, PreparedStatement}

//操作mysql 数据 初始化和释放
object MysqlUtil {
  def main(args: Array[String]): Unit = {

    val con = getConnection()
    println(con)

  }

  //建立连接
  def getConnection()={
      val connect_str = "jdbc:mysql://localhost:3306/imooc?user=root&password=root"
      DriverManager.getConnection(connect_str)
  }

  //释放相关的资源
  def release(con:Connection,state:PreparedStatement): Unit ={

    try
      if(state!=null){
        state.close()
      }
    catch {
      //打印相关堆栈的异常信息
      case e:Exception =>e.printStackTrace()
    }
    finally {
      if(con!=null){
        con.close()
      }
    }
  }

}
