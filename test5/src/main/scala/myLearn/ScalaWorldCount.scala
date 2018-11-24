package myLearn

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hys on 2018-11-06.
  */
object ScalaWorldCount{
  // 调用apply方法，方便实例化伴生类
  def apply(): ScalaWorldCount = new ScalaWorldCount()
  // 实例化伴生类
  val swc = ScalaWorldCount()
  // 定义程序入口
  def main(args: Array[String]): Unit = {
    // 如果有输入参数就返回第一个参数，并调用词频统计程序
    val params = if(args.length != 0){
      println(args(0))
      swc.worldCount(args(0))
      args(0)
    }
    else {
      // 如果没有输入参数就提示没参数输入并返回“no parameters”
      println("no parameters input")
      "no parameters"
    }
    println(params)
  }
}
class ScalaWorldCount {
  def worldCount(fileP:String): Unit ={
    // 创建SparkConf实例
    val conf = new SparkConf()
      // 设置App名称
      .setAppName("scalaWorldCount")
      // 配置本地Spark
      .setMaster("local")
    // 创建SparkContext 实例
    val sc = new SparkContext(conf)
    // 读取文本文件
    val lines = sc.textFile(fileP)
    // 把读取的内容分割成单个的单词
    val words = lines.flatMap( line => line.split(" "))
    // 把单词映射成(k, v)的形式
    val pairs = words.map( word => (word, 1))
    // 使用reduceByKey统计单词出现的次数
    val worldCounts = pairs.reduceByKey(_ + _)
    // 输出统计结果
    worldCounts.foreach(wordCount => println(wordCount._2 + "次\t" + wordCount._1))
  }
}
