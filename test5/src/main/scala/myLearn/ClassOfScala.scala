package myLearn

/**
  * Created by hys on 2018-12-08.
  */
class ClassOfScala {  // 定义一个类
  private val name = "Hys"  // 类中定义一个变量
  // 在类里面叫方法，在类外面叫函数l
  def sayHello(){println("Hello " + name)}  //定义一个方l法
  def getName = name  // 定义一个不带括号的方法r
}
object ClassOfScala{
  def main(args: Array[String]): Unit = {
    println("HelloWord")
    val test = new ClassOfScala  // 创建一个类对象
    test.sayHello()  // 调用方法
    println(test.getName) // 如果定义方法时不带括号，则调用时也不带括号
  }
}
