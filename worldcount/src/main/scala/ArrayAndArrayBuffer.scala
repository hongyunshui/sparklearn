/**
  * Created by hys on 2018-11-03.
  */

object ArrayAndArrayBuffer{
  // 定义apply函数
  def apply(): ArrayAndArrayBuffer = new ArrayAndArrayBuffer()
  // 创建main函数用来测试
  def main(args: Array[String]): Unit = {
    // 实例化一个ArrayAndArrayBuffer
    val test = ArrayAndArrayBuffer()
    test.test()
  }
}

/**
  * 关于Array和Array Buffer的练习
  */
class ArrayAndArrayBuffer(){
  // 数组初始化
  // val filedName = new Array[filedType](length)
  val a = new Array[Int](6)
  // 直接使用Array()创建数组，元素类型自动推断
  val aa = Array("Hello","world")

  def test(): Unit ={
    println(a)
    println(aa)
  }

}