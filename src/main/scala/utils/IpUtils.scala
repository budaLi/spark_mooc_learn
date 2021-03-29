package utils
import com.ggstar.util.ip.IpHelper

// 调用第三方ip地址查询库
//maven 上传本地jar包的命令
// mvn install:install-file -DgroupId=com.ggstar
//                          -DartifactId=ipdatabase
//                          -Dversion=1.0 -Dpackaging=jar
//                          -Dfile=E:\桌面\工作算法\ipdatabase-master\target\ipdatabase-1.0-SNAPSHOT.jar
object IpUtils {
    // unit代表没有返回值
    def getCity(ip:String): String ={
      //      IpHelper.findRegionByIp(ip)
      val region: String = "北京"
      return region
    }

  def main(args: Array[String]): Unit = {
    println(getCity("58.30.15.255"))
  }
}
