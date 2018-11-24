package myLearn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-14.
  */
object ActionTest {
  // 定义apply函数
  def apply(): ActionTest = new ActionTest()

  // 定义main函数
  def main(args: Array[String]): Unit = {
    val act = ActionTest()
    println("************reduce*****************")
    act.reduceTest()
    println("************collect*****************")
    act.collectTest()
    println("************count*****************")
    act.count_test()
    println("************take********************")
    act.take_test()
    println("************saveAsTexFile_test********************")
   // act.saveAsTexFile_test()
    println("***************countByKey*****************")
    act.countByKey_test()
  }
  val conf = new SparkConf()
    .setAppName("myLearn.ActionTest")
    .setMaster("local")
  val sc = new SparkContext(conf)
}

/**
  * action操作实战
  */
class ActionTest() {
  /**
    * 累加集合中的数字
    * reduce 本质就是一个聚合，将多个元素聚合成一个元素
    */
  def reduceTest(): Unit ={
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    // 并行化集合创建RDD
    val numListRDD = ActionTest.sc.parallelize(numList)
    // 求累加和
    val sumNum = numListRDD.reduce(_ + _)
    println(sumNum)
    val num2 = numListRDD.map(num => num * 2)
    // 使用foreach
    num2.foreach(num => println(num))
  }

  /**
    *   使用collect,将分布再远程上的num RDD 的数据拉取到本地。
    *   但是一般不建议这么做，因为RDD如果数据量比较打的化，执行性能会比较差，另外还可能发生内存溢出
    *   所以通常建议使用foreach进行操作
    */
  def collectTest(): Unit ={
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    // 并行化集合创建RDD
    val numListRDD = ActionTest.sc.parallelize(numList)
    val num2 = numListRDD.map(num => num * 2)

    val num22 = num2.collect()
    println("+++++++++")
    for(num <- num22) println(num)
  }

  /**
    * count:统计RDD中元素个数
    */
  def count_test(): Unit ={
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    // 并行化集合创建RDD
    val numListRDD = ActionTest.sc.parallelize(numList)
    val cont = numListRDD.count()
    println(cont)

  }

  /**
    *take: 与collect类似，都是从远程集群上获取RDD的数据，collect获取的是所有数据，take是获取钱n个元素
    */
  def take_test(): Unit ={
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    // 并行化集合创建RDD
    val numListRDD = ActionTest.sc.parallelize(numList)
    val takeDates = numListRDD.take(3)
    for(num <- takeDates) println(num)
  }

  /**
    *saveAsTexFile: 将RDD中的数据保存在文件中
    * 只能指定文件夹，也就是目录，实际上会保存为目录中的/dirName/part-0000
    * 同时不能指定已经存在的文件夹
    */
  def saveAsTexFile_test(): Unit ={
    val numList = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    // 并行化集合创建RDD
    val numListRDD = ActionTest.sc.parallelize(numList)
    val num2 = numListRDD.map(num => num * 2)
    // 直接将RDD中的数据保存再文件中
    // 只能指定文件夹，也就是目录，实际上会保存为目录中的/dirName/part-0000
    num2.saveAsTextFile("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\satf.txt")
  }
  /**
    * countByKey: 对每一个元素的Value进行统计
    */
  def countByKey_test(): Unit ={
    val soreList = Array(("leo", 66),("hys", 88),("george", 99),("jue", 86),("hys",99))
    val soreListRDD = ActionTest.sc.parallelize(soreList)
    val cntValue = soreListRDD.countByKey()
    println(cntValue)

  }

  /**
    * foreach :遍历RDD中的每一个元素，并对其进行操作
    */
  def foreach_test(): Unit ={
  }
}
