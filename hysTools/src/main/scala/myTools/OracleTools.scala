package myTools

import org.apache.spark.sql.SparkSession
/**
  * Created by Administrator on 2018/8/21.
  */
object OracleTools {
  def apply(): OracleTools = new OracleTools()

  def main(args: Array[String]): Unit = {
    val gt = OracleTools()
    gt.get_itemSet_RDD("FP_STATION_NAME_LINES", "lineno")
  }

  Class.forName("oracle.jdbc.driver.OracleDriver")
  //创建url字符串
  val url = "jdbc:oracle:thin:test/test@//192.168.1.238:1521/orcl"
  //创建SparkSession
  val spark = SparkSession
    .builder()
    .appName("oracle_spark")
    .master("local[*]")
    .getOrCreate()
}
class OracleTools(){
  def get_itemSet_RDD(tb:String, tbField:String) ={

    val jdbcDF = OracleTools.spark.read.format("jdbc").options(Map("url" -> OracleTools.url,
      "user" -> "test",
      "password" -> "test",
      "dbtable" -> tb)).load()
    jdbcDF.createOrReplaceTempView("table1")
//    val sqlWords:String = " select " + tbField + " from table1 "
    OracleTools.spark.sql(" select " + tbField + " from table1 ").show(100)
    val readOracleToRDD = OracleTools.spark.sql(" select " + tbField + " from table1 ").rdd
    readOracleToRDD
  }


}