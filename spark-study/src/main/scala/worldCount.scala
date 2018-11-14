import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-10-31.
  */
object worldCount {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt")
    val words = lines.flatMap(x => x.split("") )
    val pairs = words.map{ word => (word,1)}
    val worldCounts = pairs.reduceByKey( _ + _)
    worldCounts.foreach(worldCount => println(worldCount._1 + " appeared :" + worldCount._2 + " times"))
  }

}
