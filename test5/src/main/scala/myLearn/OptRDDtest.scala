package myLearn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-10.
  * Spark支持两种RDD操作:transformation和action
  * transformation操作会针对已有的RDD创建一个新的RDD，具有lazy特性，如果Spark应用中只定义transformation，则即使运行程序
  * 这些操作也不会执行，它不会出发Spark程序的执行，知识记录RDD的操作。
  * action主要是对RDD进行最后的操做，会触发一个Spark job的运行，从而触发这个action之前的所有transformation的执行。
  *
  * Spark通过transformation的lazy特性，来进行Spark底层的执行优化，避免产生过多的中间结果。
  */
object OptRDDtest{
  def apply(): OptRDDtest = new OptRDDtest()
}
class OptRDDtest() {
  def 每行出现的次数(): Unit ={
    val conf = new SparkConf()
      .setAppName("testAPP")
      .setMaster("local")
    val sc = new SparkContext(conf)
    // 从本地读取文件创建初始RDD
    val lines = sc.textFile("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt")
    // 将每一行映射成(k, v)的形式
    val pairs = lines.map(line => (line, 1))
    // 统计每行出现的次数 ，从创建初始RDD到此都为transformation操作
    val linesCounts = pairs.reduceByKey( _ + _)
    // 输出统计结果 ，其中foreach为action操作
    linesCounts.foreach(lineCount =>println(lineCount._2 + "次数 -->" +  "行" + lineCount._1))
    linesCounts.foreach(linecount => if (linecount._2 > 10) println(linecount._2 + "次数 -->" +  "行" + linecount._1))

  }
}
