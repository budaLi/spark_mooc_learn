# spark_mooc_learn
spark 慕课网日志分析
### 2021-3-25

勉强模仿着可以跑得项目加上百度搜到的教程，基础环境搭建成功....慢慢走上学习大数据的正轨...

### 2021-3-29 

maven项目创建，修改基本学会了，引入的第三方包在我的电脑中是中文路径...放弃了，mvn增加第三方库的两种方式。
1. clone源码后mvn 打包再上传到本地mvn仓库。
2. 下载源码，放到本地文件目录。

### 2021-3-31

通过spark sql 及dataframe查询数据，并写入mysql数据。

### 2021-4-1 

流量topN的实现及入库。
再后面就是数据可视化相关的内容，暂时不学习了，开始新的项目。

-------------------------------------------

### 2021-4-6

在本项目的代码基础上学习基于spark的电影实时和离线推荐系统。主要技术栈包括Spark,hadoop,Kafka,Hive,Zeppelin等
在这个项目中，主要有以下几层：
1.存储层：HDFS作为底层存储，Hive作为数据仓库。
2.离线数据处理:SparkSql
3.实时数据处理：Kafka,SparkStreaming 
4.数据应用层：MLlib
5.数据展示和对接：Zeppelin

开发的重难点：
1.数据仓库的准备
2.数据的处理
3.实时数据流

### 2021-4-7
1. 对links,movies,ratings,tags的数据清洗。
2. git commit后但未push的版本回退：VCS->reset head ->填写HEAD~n  n代表回退的版本 

### 2021-4-8 

配置了Hive好久...太坑，还是不行


### 2021-4-13

考虑放弃Hive,数据存储采用Hdfs+Mysql 
spark源码解析系列：https://github.com/lw-lin/CoolplaySpark