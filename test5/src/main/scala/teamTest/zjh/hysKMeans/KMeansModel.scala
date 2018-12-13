package teamTest.zjh.hysKMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
/**
  * Created by hys on 2018-12-08.
  * KMeans聚类模型。按照最临近原则把待分类样本点份到各个簇
  */
class KMeansModel() {

//  val clusterCenters:Array[Vector] extends Saveable with Serializable with PMMLExportable{
//    /**java 的构造函数*/
//    def this(centers:java.lang.Iterable[Vector]) = this(centers.asScala.toArray)
//
//    /** 聚类个数*/
//    def k:Int = clusterCenters.length
//
//    /**预测样本点属于哪个类*/
//    def predict(point: Vector):Int = {
//      KMeans.findCloses(clusterCentersWithNorm, new VectorWithNorm(point))._1
//    }
//  }
  /**
    * 模型加载
    */
  def load(){}

  /**
    * 模型的保存
    */
  def save(){}

  /**
    * 预测样本属于哪个类
    */
//  def predict(points:RDD[Vector]):RDD[Int] = {
//    //TODO
//    val centersWithNorm = clusterCenterEithNorm
//    val bcCentersWithNorm = points.context.broadcast(centersWithNorm)
//    points.map(p => KMeans.findCloses(bcCentersWithNorm, value, new VectorWithNorm(p)._1))
//  }

}
