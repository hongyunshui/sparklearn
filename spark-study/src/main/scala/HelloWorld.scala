/**
  * Created by hys on 2018-09-08.
  */
import scala.collection.mutable.ArrayBuffer

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello World")
    val sort = new HelloWorld
    sort.sort()
  }
}

class HelloWorld{
  /**
    * 给数组排序
    */
  def sort(): Unit ={
    //声明一个ArrayBuffer并赋值
    val a = ArrayBuffer(1,2,3,45,6,7,8,9)
    //对ArrayBuffer进行排序，把结果存入aSorted
    val aSorted = a.sorted
    println(aSorted)
    val b = Array(1,2,3,6,8,2,9)
    val bSort = b.sorted
    for (bs <- bSort) println(bs)
  }
}
