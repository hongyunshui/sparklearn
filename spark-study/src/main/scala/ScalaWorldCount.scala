import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-06.
  */
object ScalaWorldCount{
  //def apply: ScalaWorldCount = new ScalaWorldCount()
  def main(args: Array[String]): Unit = {
//    val fPa = args(0)
//    val fp = if(args.length == 0) {"E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt"} else fPa
    val fp = "/wordcounttest/worldCountTest.txt"
   // val fp = "E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt"
    val swc = new ScalaWorldCount()
    swc.worldCount(fp)
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
    println("########################scalaWordCount****666666666666666666666*******")
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
    println("***********888888888888888888***scalaWordCount*******************")

  }

}
