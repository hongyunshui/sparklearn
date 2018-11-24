import myLearn.{OptRDDtest, ScalaTest2, ScalaWorldCount}

/**
  * Created by hys on 2018-11-07.
  */
object ScalaTest{
  // 定义main函数实现程序入口
  def main(args: Array[String]): Unit = {
    // 程序入口参数处理
    val params:String = if(args.length != 0) {
      println(args)
      args(0)
    }
    else {
      println("args is null")
      "无参数"
    }
    println(params)
    val st = new ScalaTest
    // 同一个工程对不同类的调用
//    st.test2()
    // 词频统计
//    st.testWordCount()
    // 统计每一行出现的次数，transformation和action原理测试
    st.ta测试()
  }
}
class ScalaTest {
  // 调用同一个工程其他类测试
  def test2(): Unit ={
    val t2 =  ScalaTest2() //调用apply方法实例化ScalaTest2类
    t2.tt2()
  }
  // 词频统计测试
  def testWordCount(): Unit ={
    val wc = ScalaWorldCount()
    wc.worldCount("aaa")
  }
  def ta测试(): Unit ={
    val ta = OptRDDtest()
    ta.每行出现的次数()
  }
}
