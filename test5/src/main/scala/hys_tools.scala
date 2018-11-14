import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-13.
  */
object hys_tools{

}
class hys_tools {
  /**
    * 创建SparkContext对象
    * @return
    */
  def getSparkContext(): SparkContext ={
    val conf: SparkConf = new SparkConf()
    val sc = new SparkContext(conf)
    sc
  }

}
