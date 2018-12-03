package teamTest.zjh

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 对Oracle数据库进行读写操作
  * Created by hys on 2018-11-24.
  */
object ReadWriteOracle {
  def apply(): ReadWriteOracle = new ReadWriteOracle()
  def main(args: Array[String]): Unit = {
    val rwo = ReadWriteOracle()
//    rwo.readFromOracle()
    rwo.writeToOracle()
  }
}
class ReadWriteOracle() {
  def readFromOracle(): Unit ={
    Class.forName("oracle.jdbc.driver.OracleDriver")
    //创建url字符串
    val url = "jdbc:oracle:thin:test/test@//192.168.1.238:1521/orcl"
    //创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("oracle_spark")
      .master("local[*]")
      .getOrCreate()

    val jdbcDF = spark.read.format("jdbc").options(Map("url" -> url,
      "user" -> "test",
      "password" -> "test",
      "dbtable" -> "TM_BUS_PASSENGER_UPDOWN_PRE")).load()
    jdbcDF.createOrReplaceTempView("table1")
    spark.sql( " select * from table1 ").show(100)
  }
  def writeToOracle(): Unit ={
    //创建url字符串
    val url = "jdbc:oracle:thin:test/test@//192.168.1.238:1521/orcl"
    //写入oracle
    val connectProperties = new Properties()
    connectProperties.put("user", "test")
    connectProperties.put("password", "test")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    val arr = Array[String]("aa","bb","cc","dd","ee")
    println(arr)
//    arr.write.mode(SaveMode.Overwrite).jdbc(url,"S_FP_ASS01_1",connectProperties)
//    model.associationRules.write.mode(SaveMode.Overwrite).jdbc(url,"S_FP_ASS01_1",connectProperties)
  }
}
