package teamTest.zjh.KMeansTest
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
/**
  * Created by hys on 2018-12-05.
  */
object KMeansTest5 {
  def main(args: Array[String]) {
    // 如果参数长度小于5退出执行
    if (args.length < 5) {
      println("Usage:KMeansClustering trainingDataFilePath testDataFilePath numClusters numIterations runTimes")
      sys.exit(1)
    }

    val conf = new
        SparkConf()
      .setAppName("Spark MLlib Exercise:K-Means Clustering")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    /**
      * Channel Region Fresh Milk Grocery Frozen Detergents_Paper Delicassen
      * 2 3
      * 12669 9656 7561 214 2674 1338
      * 2 3 7057 9810 9568 1762 3293 1776
      * 2 3 6353 8808
      * 7684 2405 3516 7844
      */

      // 从数据源获得训练数据
    val rawTrainingData = sc.textFile(args(0))
    // 对训练数据进行初始化变换
    val parsedTrainingData =
      rawTrainingData.filter(!isColumnNameLine(_)).map(line => {

        Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
      }).cache()

    // Cluster the data into two classes using KMeans

    // K 值
    val numClusters = args(2).toInt
    // 迭代次数
    val numIterations = args(3).toInt
    // 执行次数
    val runTimes =
      args(4).toInt
    var clusterIndex: Int = 0
    // 创建模型
    val clusters: KMeansModel =
      KMeans.train(parsedTrainingData, numClusters, numIterations, runTimes)
//      KMeans.train(parsedTrainingData, numClusters, numIterations)

    println("簇中心数量是:" + clusters.clusterCenters.length)

    println("簇中心详细信息如下:")
    // 打印簇中心
    clusters.clusterCenters.foreach(
      x => {

        println("簇中心 " + clusterIndex + "是:")

        println(x)
        clusterIndex += 1
      })
    // 计算训练误差
    val cost = clusters.computeCost(parsedTrainingData)
    // 打印训练误差
    println("误差结果是：" + cost)

    //begin to check which cluster each test data belongs to based on the clustering result

    // 读取测试数据
    val rawTestData = sc.textFile(args(1))
    // 对测试数据进行转换
    val parsedTestData = rawTestData.filter(!isColumnNameLine(_)).map(line => {

      Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))

    })
    // 打印测试结果
    parsedTestData.collect().foreach(testDataLine => {
      val predictedClusterIndex: Int = clusters.predict(testDataLine)

      println("对象 " + testDataLine.toString + " 的簇中心是 " +
        predictedClusterIndex)
    })

    println("Spark MLlib K-means clustering test finished.")
  }

  private def
  isColumnNameLine(line: String): Boolean = {
    if (line != null &&
      line.contains("Channel")) true
    else false
  }
}
