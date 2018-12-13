package teamTest.fxx.KMeansTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by hys on 2018-12-05.
  */
object TEST {
  def main(args: Array[String]): Unit = {

    //创建spark对象
    val conf = new SparkConf().setAppName("KMeans" ).setMaster("local[*]")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    //从hdfs读取样本数据，为LIBSVM格式
    val data = sc.textFile("/fxx/knntest.txt")
    val parseData = data.map(s  => Vectors.dense(s.split('\t').map(_.toDouble))).cache()

    //创建KMeans聚类模型，并训练
    //初始化模式
    val initMode = "k-means||"

    //初始簇中心
    val numClusters = 2
    //迭代次数
    val numIterations = 20

    val model = new KMeans()
      //设置初始化模式
      .setInitializationMode(initMode)
      .setK(numClusters)
      .setMaxIterations(numIterations)
      .run(parseData)

    //误差计算
    val WSSSE = model.computeCost(parseData)
    println("within set sum of squaraed errors = "+ WSSSE)

    //打印簇中心
    val jieguoji = model.clusterCenters
    jieguoji.foreach(e =>println(e))

  }
}
