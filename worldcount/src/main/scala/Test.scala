/**
  * Created by hys on 2018-09-08.
  */
// 伴生对象
object Test{
  //apply的定义
  def apply(): Test = new Test()
  val name:String = "Hys"
  var age = 0
//工程入口
  def main(args: Array[String]): Unit = {
    val t =  Test()
    t.display()
    t.getName

  }
}

//伴生类
class Test(){
//  println(Test.name)
//  println(Test.age)
  def getName= Test.name
  def display(): Unit ={
    println(Test.name)
    print(Test.age)
  }
  def run_mysql(): Unit ={

  }
}


