package teamTest.zjh.hysKMeans

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by hys on 2018-12-08.
  * 该对象是建立KMeans聚类模型的入口，其主要定义训练KMmeans聚类模型的train方法。train方法通过设置训练参数进行训练
  */
object KMeans {
  val RANDOM = "random"
  val K_MEANS_PARALLEL = "k-means||"

  /**
    * 根据给定的参数集训练一个KMeans模型
    *
    * @param data 数据样本，格式为RDD[Vector]
    * @param k 聚类数量
    * @param maxIterations 最大迭代次数
    * @param runs 并行计算数，默认为1。返回最佳模型
    * @param initializationMode 初始化中心，支持random或者k-means++（默认）
    * @param seed 初始化时的随机种子
    * @return 返回KMeansModel对象
    */
  def train(data:RDD[Vector],
            k:Int,
            maxIterations:Int,
            runs:Int,
            initializationMode:String,
            seed:Long):KMeansModel = {
    //TODO
  new KMeansModel
  }

  /**
    * 根据给定的参数集训练一个KMeans模型
    *
    * @param data 数据样本，格式为RDD[Vector]
    * @param k 聚类数量
    * @param maxIterations 最大迭代次数
    * @param runs 并行计算数，默认为1。返回最佳模型
    * @param initializationMode 初始化中心，支持random或者k-means++（默认）
    * @return 返回KMeansModel对象
    */
  def train(data:RDD[Vector],
            k:Int,
            maxIterations:Int,
            runs:Int,
            initializationMode:String
           ):KMeansModel = {
    //TODO
    new KMeansModel
  }

  /**
    * 根据给定的参数及默认参数训练一个KMeans模型
    *
    * @param data 数据样本，格式为RDD[Vector]
    * @param k 聚类数量
    * @param maxIterations 最大迭代次数
    * @return 返回KMeansModel对象
    */
  def train(data:RDD[Vector],
            k:Int,
            maxIterations:Int
           ):KMeansModel = {
    train(data, k, maxIterations,1, K_MEANS_PARALLEL)
  }

  /**
    * 根据给定的参数集训练一个KMeans模型
    *
    * @param data 数据样本，格式为RDD[Vector]
    * @param k 聚类数量
    * @param maxIterations 最大迭代次数
    * @param runs 并行计算数，默认为1。返回最佳模型
    * @return 返回KMeansModel对象
    */
  def train(data:RDD[Vector],
            k:Int,
            maxIterations:Int,
            runs:Int
           ):KMeansModel = {
    train(data, k, maxIterations, runs,K_MEANS_PARALLEL)
    //TODO
    new KMeansModel
  }

  /**
    * findCloses静态方法，找到当前点距离最近的聚类中心。返回结果是一个元祖(Int, Doublt)。其中Int表示的是聚类
    * 中心点的索引，Doublt表示的是距离。
    */
  def findCloses(){}

  /**
    * 内部调用了findClosest方法，返回的是findClosest方法元祖的第二个值，表示的是cost距离。
    */
  def pointClosest(){}

  /**
    * 调用了MLUtils类的工具方法fastSquareDistance来快速的计算距离。
    */
  def fastSquaredDistance(){}

  /**
    * 检查初始化中心点的模式，是random模式还是Kmeans++模式。
    */
  def validateInitMode(){}

}
class KMeans private(){

}
