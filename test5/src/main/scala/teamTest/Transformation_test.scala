package teamTest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hys on 2018-11-13.
  */
object Transformation_test{
  // 调用apply函数
  def apply(): Transformation_test = new Transformation_test()
  // 创建SparkConf对象
 final val conf = new SparkConf()
    // 设置App名称
    .setAppName("开发实战")
    // 设置Spark运行模式
    .setMaster("local")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val tr = Transformation_test()
    tr.map_test()
    println("***********")
    tr.filter_test()
    println("***********flatMap MYFP**********")
    tr.flatMap_test()
    println("******groupByKey操作*******")
    tr.groupByKey_test()
    println("**********reduceByKey*********")
    tr.reduceByKey_test()
    println("**********sortByKey**********")
    tr.sortByKey_test()
    println("***********joinAndcogroup*****")
    tr.joinAndCogroup()
  }
}
class Transformation_test(){
  /**
    * map算子案例：将集合中每一个元素都乘以2
    * map算子： 是对任何类型的RDD，都可以调用
    */
  def map_test(): Unit ={
    // 构造集合
    val numbers = Array(1, 2, 3, 5, 6, 7, 8, 9)
    // 并行化集合创建初始化RDD
    val numberRDD = Transformation_test.sc.parallelize(numbers)
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
    val numbersRDD = Transformation_test.sc.parallelize(numbers)
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
    val linesRDD = Transformation_test.sc.parallelize(lines)
    // 使用flatMap算子把每一行拆分成多个单词
    val words = linesRDD.flatMap(lines => lines.split(" "))
    // 打印结果
    words.foreach( word => println(word))
  }

  /**
    * groupByKey算子: 把具有形同key的元素分为一组。
    * 案例：按照班级对乘积分组
    */
  def groupByKey_test(): Unit ={
    // 模拟集合
    val scores = Array(Tuple2("class1", 80),Tuple2("class2", 90),Tuple2("class1", 75),
                        Tuple2("class3", 95), Tuple2("class1", 70))
    // 并行化集合，创建初始化RDD
    val scoresRDD = Transformation_test.sc.parallelize(scores)
    // 对scoresRDD 进行groupByKey操作
    val groupScores = scoresRDD.groupByKey()
    // 打印结果
    groupScores.foreach(classScores => {println(classScores)
            println(classScores._1)
            classScores._2.foreach(sc => println(sc))
            println("**********************")}
            )
  }

  /**
    * reduceByKey算子：reduceByKey 接收的参数是Function2类型，它有三个泛型参数，分别代表了三个值，第一个和第二个代表了
    * 原始RDD中的元素的Value的类型，对每个key进行reduce都会依次将第一个和第二个value传入，将值再与第三个value传入。
    * 第三个泛型类型，代表了每次reduce操作返回的值的类型，默认是与原始RDD的value类型相同的。
    * reduceByKey算法返回的RDD，还是传入的参数的DD的类型
    */
  def reduceByKey_test(): Unit ={
    // 模拟集合
    val scores = Array(Tuple2("class1", 80),
          Tuple2("class2", 90),Tuple2("class1", 75),
          Tuple2("class3", 95), Tuple2("class1", 70))
    // 并行化集合创建
    val scoresPairRDD = Transformation_test.sc.parallelize(scores)
    // 执行reduceByKey，累加得到每个班级的总分
    val totalScore = scoresPairRDD.reduceByKey(_ + _)
    // 打印每个班级的总分
    totalScore.foreach(sp => println(sp))
  }

  /**
    * sortByKey算子：根据key进行排序，可以手动指定排列顺序，返回的元素内容都是一样的，但是RDD的顺序不一样了
    * 案例：按照分数排序
    */
  def sortByKey_test(): Unit ={
    val soreList = Array(("leo", 66),("hys", 88),("george", 99),("jue", 86))
    val soreListRDD = Transformation_test.sc.parallelize(soreList)
    val scoresSort = soreListRDD.sortByKey(ascending = false)
    scoresSort.foreach(score => println(score))
  }

  /**
    * join
    * cogroup
    * 案例：打印学生成绩
    */
  def joinAndCogroup(): Unit ={
    val studentList = Array(Tuple2(1, "leo"),Tuple2(2, "hys"),Tuple2(3, "george"),Tuple2(5, "jue"), Tuple2(6, "jue"))
    val scoreList = Array(Tuple2(1, 81),Tuple2(2, 72),Tuple2(3, 83),Tuple2(5, 74),Tuple2(3, 88), Tuple2(2, 60))
    // 并行化连个
    val studentListRDD = Transformation_test.sc.parallelize(studentList)
    val scoreListRDD = Transformation_test.sc.parallelize(scoreList)
    // 使用join算子关联两个RDD
    // join以后，会根据key 进行join，返回的RDD的每一个元素就是通过Key join上的一个Pair。RDD
    // join 是没匹配到一个key 就生成一个元素。相当于Sql中的内连接
    val studentScores = studentListRDD.join(scoreListRDD)
    // 打印结果
    studentScores.foreach(sts => println(sts ))
    println("*********==========")
    // cogroup与join是把匹配到的相同的key的元素都放到了同一个元素当中
    // 与sql中的外连接类似，但不完全相同
    val ssc = studentListRDD.cogroup(scoreListRDD)
    ssc.foreach(ssc => println(ssc))

  }
}
