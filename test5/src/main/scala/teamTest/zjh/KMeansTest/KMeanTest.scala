package teamTest.zjh.KMeansTest

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hys on 2018-12-05.
  */
object KMeanTest{
  def apply: KMeanTest = new KMeanTest()

}
class KMeanTest {
  def kt(): Unit ={
    // 构建Sprak对象
    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    //  读取数据样本，为LIBSVM格式
//    val data = myTools.OracleTools().get_itemSet_RDD("FP_CARDID_STATIONS_HYS", "GROUP_STATION")
//    val parsedData = data.map(x => Vector.dense(x.getString(0).split(",")))

    // 新建KMeans聚类模型，并训练
    val initMode = "k-means||"
    val numClusters = 2
    val numIterations = 20
//    val model = new KMeans()
//      .setInitializationMode(initMode)
//      .setK(numClusters)
//      .setMaxIterations(numIterations)
//      .run()

  }
}
