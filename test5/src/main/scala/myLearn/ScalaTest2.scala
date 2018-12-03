package myLearn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-07.
  */
object ScalaTest2{
  // 实现apply方法，方便class ScalaTest2的实例化
  def apply(): ScalaTest2 = new ScalaTest2()
  // 伴生类和伴生对象可以相互调用private field
  private val name = "hys"
//  val conf = new SparkConf()
//  val sc = new SparkContext(conf)

//  val arr = Array(13,15,16,16,19,20,20,21,22,22,25,25,25,25,30,33,33,35,35,35,36,40,45,46,52,70)
//  val arrRdd = sc.paral
//  val av = arrRdd.reduce(_ + _)/arr.length
//  val avv = av.take(1)
}
class ScalaTest2 {
  def tt2(): Unit ={
    println("Hello t2!!!!你好!!!!!!Hello MavenAssembly!!!!!。。。。。。。。。" + ScalaTest2.name )
  }

}
