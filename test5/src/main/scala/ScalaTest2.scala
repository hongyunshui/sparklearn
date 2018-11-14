/**
  * Created by hys on 2018-11-07.
  */
object ScalaTest2{
  // 实现apply方法，方便class ScalaTest2的实例化
  def apply(): ScalaTest2 = new ScalaTest2()
  // 伴生类和伴生对象可以相互调用private field
  private val name = "hys"
}
class ScalaTest2 {
  def tt2(): Unit ={
    println("Hello t2!!!!你好!!!!!!Hello MavenAssembly!!!!!。。。。。。。。。" + ScalaTest2.name )
  }

}
