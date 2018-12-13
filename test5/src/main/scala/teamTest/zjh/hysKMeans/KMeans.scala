package teamTest.zjh.hysKMeans

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
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

/**
  * kMeans聚类算法，支持并行计算及k-means++的初始化算法
  * 这是一个迭代算法，样本根据RDD应该被缓存
  *
  *
  * @param k 聚类个数
  * @param maxIterations 迭代次数
  * @param runs 并行度
  * @param initializationMode 初始中心算法
  * @param initializationSteps 初始步长
  * @param epsilon 中心距离阈值
  * @param seed 随机种子
  */
class KMeans private(
                      private  var k:Int,
                      private  var runs:Int,
                      private  var maxIterations:Int,
                      private  var initializationMode:String,
                      private  var initializationSteps:Int,
                      private  var epsilon:Double,
                      private  var seed:Long
                    )extends Serializable with Logging{

  /**
    * 构建KMeans 实例的默认参数：{k:2,maxIterations:20,runs:1,
    * initializationMode:"k-means||",initializationSteps:5,epsilon:1e-4,seed:random}
    * @return
    */

  def this() = this(2,20,1,KMeans.K_MEANS_PARALLEL,5,1e-4,Utils.random.nextLong())

  /**
    * 聚类个数
    * @return
    */
  def getK :Int = k

  /**
    * 设置聚类个数，默认：2
    */
  def setK(k:Int):this.type = {
    this.k = k
    this

  }

  /**
    * 最大的迭代次数
    *
    */
  def getMaxIterations:Int = maxIterations

  /**
    * 设置最大迭代次数，默认：20
    */
  def setMaxItetations(maxIterations:Int):this.type ={
    this.maxIterations = maxIterations
    this

  }
  /*
  初始中心算法，支持random 或则k-means++
   */
  def getInitalizationMode :String = initializationMode

  /**
    * 设置初始中心算法，可以选择random模式，选择随机点来初始化中心
    * 也可以选择k-means++模式，参照：
    * （Bahmani et al.,Scalable K-Means++,VLDB 2012）。默认：k-means++
    *
    */
  def setInitializationMode(initializationMode:String):this.type = {
    if (initializationMode !=KMeans.RANDOM && initializationMode !=KMeans.K_MEANS_PARALLEL){
      throw new IllegalArgumentException("Invalid initialization mode :" + initializationMode)
    }
    this.initializationMode = initializationMode
    this
  }

  /**
    * ::Experimental ::
    * 并行执行的并行算法的数量
    */

  @Experimental
  def getRuns:Int = runs

  /**
    * ::Experimental ::
    * 设置并行计算的数量，默认：1
    */

  @Experimental
  def setRuns(runs:Int) :this.type = {
    if (runs <=0){
      throw new IllegalArgumentException("Number of runs must be positive")

    }
    this.runs = runs
    this

  }

  /**
    * 初始化时的初始步长
    */
  def getInitializationSteps:Int = initializationSteps

  /**
    * 设置初始步长，用于k-means++初始化模型，默认：5
    */
  def setInitializationSetps(initializationSteps :Int) :this.type  = {
    if (initializationSteps <=0) {
      throw new IllegalArgumentException("Number of initialization steps must be positive")
    }
    this.initializationSteps = initializationSteps
    this

  }

  /**
    * 中心距离阈值
    */

  def  getEpsilon :Double = epsilon

  /**
    * 设置中心距离阈值
    * 如果所有的中心移动距离小于这个距离阈值，我们将停止选择迭代运行
    *
    */
  def setEpsilon(epsilon :Double):this.type  = {
    this.epsilon = epsilon
    this

  }

  /**
    * 聚类初始化的随机种子
    */
  def getSeed :Long = seed

  /**
    * 设置聚类初始化的随机种子
    *
    */
  def setSeed (seed:Long):this.type ={
    this.seed = seed
    this

  }
}
