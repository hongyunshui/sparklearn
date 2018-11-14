import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-09.
  */
object CreateRDDtest{
  // 定义apply函数，方便以 "val = CreateRDDtest()" 的形式实例化
  def apply(): CreateRDDtest = new CreateRDDtest()
  def main(args: Array[String]): Unit = {
    val ct = CreateRDDtest()
    val sc = ct.creatSparkContext()
    //并行化集合创建RDD
    ct.create_ParallelezeCollection(sc)
    //使用本地文件创建RDD
    ct.createRDDByLocalFile(sc)
  }
}
class CreateRDDtest() {
  /**
    * 通过执行程序发现如果一个class中两次实例化SparkContext就会报错
    * @return
    */
  def creatSparkContext():SparkContext ={
    // 创建SparkConf
    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local")
    // 创建SparkContext并返回
    new SparkContext(conf)
  }
  /**
    * 并行化集合创建RDD
     */
  def create_ParallelezeCollection(sc:SparkContext): Unit ={
    // 要通过并行化集合的方式创建RDD，那么就调用SparkContext以及其子类的parallelize()方法
    val nums = Array(1, 3, 5, 6, 7, 8, 9)
    val numsRDD = sc.parallelize(nums, 2)
    //执行reduce操作：相当于先把第一格跟第二个元素进行自己定义的操作，然后把结果与下一个
    // 元素进行相同的操作，依次类推一直到最后一个元素
    val sum = numsRDD.reduce( _ + _) // 计算所有元素的和
    val muls = numsRDD.reduce( _ * _) // 计算所有元素的乘积
    // 输出结果
    println(sum)
    println(muls)
  }
  /**
    * 使用本地文件创建RDD，如果是在Linux集群上使用本地文件创建RDD，每个集群节点都要有相同的本地文件。
    */
  def createRDDByLocalFile(sc:SparkContext): Unit ={
    // 本地文件读入并创建RDD
    val lines = sc.textFile("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt")
    //    val pl = sc.parallelize(("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt")
    // 统计文本文件的字数
    val lineLength = lines.map(line =>line.length)  // 计算出每一行的数字
    val wordnum = lineLength.reduce( _ + _)  // 把每一行的字数累加得到所有字数
    // 打印统计结果
    println(wordnum)
  }
}
