package teamTest.zjh.hysKMeans

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by hys on 2018-12-13.
  */
class testCode {

}
object testCode{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("KMT")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/fxx/xl_line_date.csv")
    val parsedData = data.map(s => Vectors.dense(s.split("\t").map(_.toDouble))).cache()
//    KMeans.train(parsedData,2,20,20,"modeKMTEST", Random.nextLong())
    val model = new KMeans()
      .setK(1)
      .setRuns(2)
      .setMaxItetations(8)

    println(model.getK,model.getRuns,model.getMaxIterations)
  }
}
