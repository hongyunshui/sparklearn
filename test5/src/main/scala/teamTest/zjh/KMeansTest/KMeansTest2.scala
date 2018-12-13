package teamTest.zjh.KMeansTest
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.log4j.{Level,Logger}

/**
  * Created by hys on 2018-12-05.
  */
object KMeansTest2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext("local[*]", "K-means", conf)
    val dataSet = sc.textFile("/fxx/knntest.txt")
    val parsedData = dataSet.map(s => Vectors.dense(s.split('\t').map(_.toDouble))).cache()
    //将每一组数据解析成RDD[org.apache.spark.mllib,linalg.Vector]，Array（[0.0,0.0,0.0],[1.0,1.0,1.0]）
    val numClusters = 2
    //设置簇的个数
    val numIterations = 20
    //设置迭代次数

//    val clustrs1 = KMeans.train(parsedData, 2, 10)
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    println("cluster centers:") //调用kmeans.clusterCenters遍历质点中心
    for (c <- clusters.clusterCenters) {
      println(" " + c.toString)
    }

    val cost = clusters.computeCost(parsedData) //计算簇误差
    println("误差结果是：" + cost)
    clusters.save(sc, "/KMeansTest/") //保存clusters到指定的路径
//    println("Vetors 0.2 0.2 0.2 is belongs to Clusters:" + clusters.predict(Vectors.dense("0.2 0.2 0.2".split(' ').map(_.toDouble))))
    println("Vetors 0.2 0.2  is belongs to Clusters:" + clusters.predict(Vectors.dense("0.2 0.2".split(' ').map(_.toDouble))))
    println("Vetors 5 5  is belongs to Clusters:" + clusters.predict(Vectors.dense("5 5".split(' ').map(_.toDouble))))
  }
}
class KMeansTest2 {

}
