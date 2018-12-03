package myLearn

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-28.
  */
object SparkSQLTest{
  def applyl: SparkSQLTest = new SparkSQLTest()

  /**
    * 调试代码入口
    * @param args 程序入口变量
    */
  def main(args: Array[String]): Unit = {

  }
}
class SparkSQLTest {
  def getsqlContext(): Unit ={
    val conf = new SparkConf()
      .setAppName("sparkSQL")
      .setMaster("local")
    val sparkContext = new SparkContext(conf)
    val sqlC = new SQLContext(sparkContext)
    // 导入SQLContext中的隐式转换
//    import sqlC.implicits._
  }
}
