package myTools

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-13.
  */
object HysTools{

}
class HysTools {
  /**
    * 创建SparkContext对象
    * @return
    */
  def getSparkContext: SparkContext ={
    val conf: SparkConf = new SparkConf()
    val sc = new SparkContext(conf)
    sc
  }

}
