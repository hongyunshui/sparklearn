package teamTest.zjh.hysKMeans

import org.apache.spark.annotation.Experimental
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.random.XORShiftRandom

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
    new KMeans().setK(k)
      .setMaxItetations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .setSeed(seed)
      .run(data)
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
    new KMeans()
      .setMaxItetations(maxIterations)
      .setRuns(runs)
      .setInitializationMode(initializationMode)
      .run(data)
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
  }

  /**
    * 找到点与所有聚类中心最近的一个中心
    */
//  private[mllib] def findtClosest(centers:TraversableOnce[VectorWithNorm],
//                                  point:VectorWithNorm):(Int, Double) = {
//    (1,2.0) // TODO
//  }

  /**
    * 调用了MLUtils类的工具方法fastSquareDistance来快速的计算距离
    */
  def fastSquaredDistance(): Unit ={
    println("******************fastSquaredDistance********************")
  }

  /**
    * 检查初始化中心点的模式，是random模式还是Kmeans++模式。
    */
  def validateInitMode(): Unit ={
    println("*******************validateInitMode************************")
  }


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

  def this() = this(2,20,1,KMeans.K_MEANS_PARALLEL,5,1e-4,(new Random).nextLong())

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

  /**
    *
    * 根据给定的训练数据开始训练模型，其中数据应该缓存（因为要进行迭代计算）
    * 该方法主要用runAlgorithm方法进行中心点的计算
    * @param data 要进行训练的数据集
    * @return
    */
  def run(data:RDD[Vector]):KMeansModel = {
    if (data.getStorageLevel == StorageLevel.NONE){
      logWarning("The input data is not directly cached, which may hutr performance if its " + " parent RDDs are also uncached.")
    }
    // 计算L2范数，并且缓存。zippedData格式为（向量，向量的L2范数）
    val norms = data.map(Vectors.norm(_, 2.0))
    norms.persist()
    val zippedData = data.zip(norms).map{case(v, norm) => new VectorWithNorm() }// TODO

    //运行KMeans算法
//    val model = runAlgorithm(zippedData)
    norms.unpersist()
    // 数据检测告警
    if (data.getStorageLevel == StorageLevel.NONE){
      logWarning("The input data was not directly cached,which mayy hurt performance if its  " + " parent RDD are also uncached." )
    }
//    model
    new KMeansModel // TODO
  }

  /**
    * 初始化，随机选择中心点
    * @return
    */
// private def initRandom(data:RDD[VectorWithNorm]):Array[Array[VectorWithNorm]] = {
//   // 从数据样本中，随机抽取数据作为中心点，runs是并行度，k是聚类中心数
////   val sample = data.takeSample(true,runs * k , new XORShiftRandom(this.seed).nextLong()).toSeq
//   // takeSample 是数据采样算子
//   val sample = data.takeSample(true, runs * k, new Random(this.seed).nextLong()).toSeq // 自行修改后的代码
//   // tabulate[T](n:Int)(f:(Int)=>T):Array[T] 返回包含一个给定的函数的值朝贡从0开始的范围内的整数值的数组
//   Array.tabulate(runs)(r => sample.slice(r * k,(r+1) * k).map { v =>
//     new VectorWithNorm(Vectors.dense(v.vector.toArray),v.norm) // 返回（中心向量，中心向量L2范数）
//   }.toArray)
// }

  /**
    * 初始化样本中心点，采用k-means++算法
    * @return
    */
//  private def initKMeansParallel(data: RDD[VectorWithNorm]):Array[Array[VectorWithNorm]] = {
//    // 初始化中心costs
//    val centers = Array.tabulate(runs)(r => ArrayBuffer.empty[VectorWithNorm])
//    val costs = data.map(_ => Vectors.dense(Array.fill(runs)(Double.PositiveInfinity))).cache()
//    // 初始化第一个中心点
////    val seed = new XORShiftRandom(this.seed).nextInt()
//    val seed = new Random(this.seed).nextInt() // 自行修改的代码
//    val sample = data.takeSample(true, runs, seed).toSeq
//    val newCenters = Array.tabulate(runs)(r => ArrayBuffer(sample(r).toDense))
//
//    /**合并新的中心到中心到中心*/
//    def mergeNewCenters():Unit = {
//      var r = 0
//      while(r < runs){
//        centers(r) ++= newCenters(r)
//        newCenters(r).clear()
//        r += 1
//      }
//    }
//    // 在每次迭代中，抽样2* k个样本进行计算
//    // 注意：每次迭代中计算样本点与中心点之间的距离
//    var step = 0
//    while (step < initializationSteps){
//      val bcNewCenters = data.context.broadcast(newCenters) // 新中心点
//      val preCosts = costs // 前costs
//      // costs 计算
//      costs = data.zip(preCosts).map { case (point, cost) =>
//      Vectors.dense(
//        Array.tabulate(runs){ r =>
//        math.min(KMeans.pointClosest(bcNewCenters,value(r),point), cost(r))
//        }
//      )}.cache()
//      val sumCosts = costs
//        .aggregate(Vectors.zeros(runs))(
//          seqop = (s, v) =>{
//            // s +=v
//            axpy(1.0, v, s)
//            s
//          },
//          combop = (s0, s1) => {
//            // s0 += s1
//            axpy(1.0, s1, s0)
//            s0
//          }
//        )
//      preCosts.unpersist(blocking = false)
//      // 选择中心点
//      val chosen = data.zip(costs).mapPartitionsWithIndex{ (index, pointsWithCosts) =>
//      val rand = new XORShiftRandom(seed ^ (step << 16) ^ index)
//      pointsWithCosts.flatMap{case(p, c) =>
//      val rs = (0 until runs).filter{ r =>
//      rand.nextDouble() < 2.0 * c(r) * k / sumCosts(r)
//      }
//      if (rs.length > 0 ) Some(p, rs) else None
//      }
//      }.collect()
//      mergeNewCenters()
//      chosen.foreach{ case (p, rs) =>
//      rs.foreach(newCenters(_) += p.toDense)
//      }
//      step += 1
//    }
//    mergeNewCenters()
//    costs.unpersist(blocking = false)
//    // 最后，可能会有超过k个的候选中心点，通过候选中心点对应的样本并且运行本地k-means++来选择k个中心点
//    val bcCenters = data.context.broadcast(centers)
//    val weightMap = data.flatMap{p =>
//    Iterator.tabulate(runs) { r =>
//      ((r, KMeans.findCloses(bcCenters.value(r), p)._1), 1.0)
//    }
//    }.reduceByKey(_ + _).collectAsMap()
//    val finalCenters = (0 until runs).par.map{ r =>
//    val myCenters = centers(r).toArray
//    val myWeights = (0 until myCenters.length).map(i => weightMap.getOrElse((r, i),
//      0.0)).toArray
//    LocalKMeans.KMeansPlusPlus(r, myWeights, k, 30)
//    }
//    finalCenters.toArray
//  }

}
