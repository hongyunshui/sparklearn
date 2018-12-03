//package teamTest.smy
//
//import org.apache.spark.ml.fpm.FPGrowth
//import org.apache.spark.sql.SparkSession
///**
//  * Created by Administrator on 2018/11/22.
//  */
//object  Gl {
//  def apply(): Gl = new Gl()
//  def main (args: Array[String]): Unit = {
//
//    // 实例化对象
//    val gl = Gl()
//    // 调用算法
//    gl.fpGrowthTest(spark)
//    // 获取项集
////    gl.getItemSet(spark)
//  }
//  val spark = SparkSession
//    .builder()
//    .master("local[*]")
//    .appName("fptree")
//    .getOrCreate()
//}
//class Gl(){
//  /**
//    * FP算法产生强规则
//    * @param spark 参数
//    */
//  def fpGrowthTest(spark: SparkSession): Unit ={
////    import spark.implicits._
////    val td = Seq(
////      ("M O N K E Y"),
////      ("D O N K E Y"),
////      ("M A K E"),
////      ("M U C K Y"),
////      ("C O K I E")
////    ).map(e => e.split(" "))
////    val testData = td.toDF("goods")
//    val testData = teamTest.smy.Gl( ).getItemSet(spark).map(x => x.getString(0).split(",")).toDF("goods")
//
//    val fpg = new FPGrowth()
//      .setItemsCol("goods") //指定输入列
//      .setMinSupport(0.6)  //最小支持度阈值
//      .setMinConfidence(0.8) // 最小置信度阈值
//
//    println("********代入数据*********")
//    val model = fpg.fit(testData)
//    println("******打印频繁项集********")
//    model.freqItemsets.show() //打印频率项集
//    //    +---------+----+
//    //    |    items|freq|
//    //    +---------+----+
//    //    |      [O]|   3|
//    //    |   [O, E]|   3|
//    //    |[O, E, K]|   3|
//    //    |   [O, K]|   3|
//    //    |      [K]|   5|
//    //    |      [E]|   4|
//    //    |   [E, K]|   4|
//    //    |      [M]|   3|
//    //    |   [M, K]|   3|
//    //    |      [Y]|   3|
//    //    |   [Y, K]|   3|
//    //    +---------+----+
//    println("**********打印规则*************")
//    model.associationRules.show() //打印大于最小置信度阈值的项集与项集及置信度值
//    //    +----------+----------+----------+
//    //    |antecedent|consequent|confidence|
//    //    +----------+----------+----------+
//    //    |       [Y]|       [K]|       1.0|
//    //    |    [O, K]|       [E]|       1.0|
//    //    |    [O, E]|       [K]|       1.0|
//    //    |       [O]|       [E]|       1.0|
//    //    |       [O]|       [K]|       1.0|
//    //    |       [E]|       [K]|       1.0|
//    //    |       [M]|       [K]|       1.0|
//    //    |       [K]|       [E]|       0.8|
//    //    +----------+----------+----------+
//
//    model.transform(testData).show()
//    //    +------------------+----------+
//    //    |             goods|prediction|
//    //    +------------------+----------+
//    //    |[M, O, N, K, E, Y]|        []|
//    //    |[D, O, N, K, E, Y]|        []|
//    //    |      [M, A, K, E]|        []|
//    //    |   [M, U, C, K, Y]|       [E]|
//    //    |   [C, O, K, I, E]|        []|
//    //    +------------------+----------+
//
//    spark.stop()
//  }
//
//  /**
//    * 获取项集
//    * @param sparkSession 参数
//    */
//  def getItemSet(sparkSession: SparkSession) ={
//    Class.forName("oracle.jdbc.driver.OracleDriver")
//    //创建url字符串
//    val url = "jdbc:oracle:thin:MYFP/MYFP@//192.168.1.238:1521/orcl"
//    val jdbcDF = sparkSession.read.format("jdbc").options(Map("url" -> url,
//      "user" -> "test",
//      "password" -> "test",
//      "dbtable" -> "FP_STATION_NAME_LINES")).load()
//    jdbcDF.createOrReplaceTempView("table1")
//    sparkSession.sql( " select * from table1 ").show(100)
//    val its = sparkSession.sql("select lineno from table1 ")
//    its
//  }
//}
//
