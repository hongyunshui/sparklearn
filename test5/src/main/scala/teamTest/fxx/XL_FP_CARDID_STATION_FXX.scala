package teamTest.fxx

import java.util.Properties

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{SaveMode, SparkSession, _}

/**
  * Created by fanxiaoxia on 2018/11/24.
  */
object XL_FP_CARDID_STATION_FXX {
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val url = "jdbc:oracle:thin:test/test@//192.168.1.238:1521/orcl"

  def oracleTable(spark:SparkSession,url:String,tableName:String):DataFrame = {
    val oracleDf = spark.read.format("jdbc").options(Map("url" -> url,
      "user" -> "test",
      "password" -> "test",
      "dbtable" -> tableName)).load()
    oracleDf
  }

  def spark_init(APPName: String, local: Boolean): SparkSession = {
    if (local) {
      SparkSession.builder()
        //.enableHiveSupport()
        .appName(APPName)
        //.config("spark.sql.autoBroadcastJoinThreshold",1073741824)
        .getOrCreate()
    } else {
      SparkSession.builder()
        //        .enableHiveSupport()    // 需要hive的话，就放开
        .appName(APPName)
        .master("local[*]")
        .getOrCreate()
    }
  }


  //  def arrToString(arr: Array[String]) : String ={
  //    arr.reduce(_+","+_).toString
  //  }
  def main(args: Array[String]): Unit = {

    val spark = spark_init("XL_FP_CARDID_STATION", local = false) // true 为集群模式，需要打包在集群上跑，false为测试，本地跑

    import spark.implicits._
    // 这是读取
    val cardlogDF = oracleTable(spark, url, "XL_FP_CARDID_STATION")
      .map(e => e.getString(1).split(","))

    val fpGrowth = new FPGrowth()   //创建一个FPGrowth的算法实列

      .setItemsCol("value")
      .setMinSupport(0.01)  //最小支持度阈值
      .setMinConfidence(0.1)  //最小置信度阈值
      .setNumPartitions(2)

    val model = fpGrowth.fit(cardlogDF)

    println("*****频繁项集******")
    model.freqItemsets.show(1000)  //频繁项集
    println("*****关联规则*****")
    model.associationRules.show(1000)  //关联规则


    //写入oracle
    println("******结果写入oracle*******")
    val connectProperties = new Properties()
    connectProperties.put("user", "test")
    connectProperties.put("password", "test")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()

    val a = model.freqItemsets
    a.printSchema()
    // model.associationRules.printSchema()

    model.associationRules.write.mode(SaveMode.Overwrite).jdbc(url,"XL_FP_CA_ST_ASS_HYS",connectProperties)
    model.freqItemsets.write.mode(SaveMode.Overwrite).jdbc(url,"XL_FP_CA_ST_FRE_HYS",connectProperties)
    // model.associationRules.show()
    spark.stop()
  }
}
