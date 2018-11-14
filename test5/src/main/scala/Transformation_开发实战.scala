import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-13.
  */
object Transformation_开发实战{
  // 调用apply函数
  def apply(): Transformation_开发实战 = new Transformation_开发实战()
  // 创建SparkConf对象
 final val conf = new SparkConf()
    // 设置App名称
    .setAppName("开发实战")
    // 设置Spark运行模式
    .setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val tr = Transformation_开发实战()
    tr.map_test()
    println("***********")
    tr.filter_test()
    println("***********flatMap test**********")
    tr.flatMap_test()
    println("******groupByKey操作*******")
    tr.groupByKey_test()
  }
}
class Transformation_开发实战(){
  /**
    * map算子案例：将集合中每一个元素都乘以2
    * map算子： 是对任何类型的RDD，都可以调用
    */
  def map_test(): Unit ={
    // 构造集合
    val numbers = Array(1, 2, 3, 5, 6, 7, 8, 9)
    // 并行化集合创建初始化RDD
    val numberRDD = Transformation_开发实战.sc.parallelize(numbers)
    // 使用Map算子将numberRDD 中的每个算子都乘以2
    val number2RDD = numberRDD.map(num => 2 * num)
    // 打印乘以2之后的结果
    number2RDD.foreach(num2 => println(num2))
  }

  /**
    * filter 过滤到集合中的偶数
    * filter算子： 算子传入的是一个function，如果function返回的是true则保留当前元素，否则就不保留。
    */
  def filter_test(): Unit ={
    // 构造集合
    val numbers = Array(1, 2, 3,5, 6, 7, 8, 9, 10, 15)
    // 并行化集合创建初始化RDD
    val numbersRDD = Transformation_开发实战.sc.parallelize(numbers)
    // 对初始RDD执行filter算子，过滤出偶数
    val evennumberRDD = numbersRDD.filter(number => number%2 == 0 )
    // 打印过滤后的结果
    evennumberRDD.foreach(even => println(even))
  }

  /**
    *flatMap算子： 即将接收的原始RDD中的每个元素进行逻辑的计算和处理，返回多个元素，新的RDD中封装了所有的新元素
    */
  def flatMap_test(): Unit ={
    // 构造集合
    val lines = Array("Hello world","Nice to meet you ", " Glad to meet you ")
    // 并行化集合创建RDD
    val linesRDD = Transformation_开发实战.sc.parallelize(lines)
    // 使用flatMap算子把每一行拆分成多个单词
    val words = linesRDD.flatMap(lines => lines.split(" "))
    // 打印结果
    words.foreach( word => println(word))
  }

  /**
    * groupByKey算子:
    * 案例：按照班级对乘积分组
    */
  def groupByKey_test(): Unit ={
    // 模拟集合
    val scores = Array(("class1", 80),("class2", 90),("class1", 75),("class3", 95), ("class6", 70))
    // 并行化集合，创建初始化RDD
    val scoresRDD = Transformation_开发实战.sc.parallelize(scores)
    // 对scoresRDD 进行groupByKey操作
    val groupScores = scoresRDD.groupByKey()
    // 打印结果
    groupScores.foreach(classScores => println(classScores))

  }
}
