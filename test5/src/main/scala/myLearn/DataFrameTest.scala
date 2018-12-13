package myLearn

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-12-12.
  */
class DataFrameTest {

}
object DataFrameTest{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameTest")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("/test/employees.json")
    df.show()
  }
}
