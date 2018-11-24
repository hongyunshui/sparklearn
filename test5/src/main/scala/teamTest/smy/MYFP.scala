package teamTest.smy

import java.util.Properties

import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.{SaveMode, SparkSession, _}

/**
  * Created by Administrator on 2018/11/22.
  */
object MYFP {
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val url = "jdbc:oracle:thin:test/test@//192.168.1.238:1521/orcl"
  def oracleTable(spark:SparkSession,url:String,tableName:String):DataFrame = {
    val oracleDf = spark.read.format("jdbc").options(Map("url" -> url,
                                                          "user" -> "test",
                                                          "password" -> "test",
                                                          "dbtable" -> tableName)).load()
    oracleDf
  }

  def spark_init(APPName:String, local:Boolean = false):SparkSession={
    if (local) {
      SparkSession.builder()
        .appName(APPName)
        .getOrCreate()
    }else{
      SparkSession.builder()
        .appName(APPName)
        .master("local[*]")
        .getOrCreate()
    }
  }

  def main (args: Array[String]): Unit = {
    val spark = if (args.length == 0) spark_init("fp_xulinv") else spark_init("fp_xulinv", local = true)
//    val spark = spark_init("fp_xulinv")
    import spark.implicits._
    val testDF = oracleTable(spark,url,"S_10_10F")
      .map(e => e.getString(0).split(",") )

    val fpg = new FPGrowth()
      .setItemsCol("value") //指定输入列
      .setMinSupport(0.001)  //最小支持度阈值
      .setMinConfidence(0.6) // 最小置信度阈值

    val model = fpg.fit(testDF)
    val rr = model.associationRules
    rr.show()
    val rr2 = model.freqItemsets

    //写入oracle
    val connectProperties = new Properties()
    connectProperties.put("user", "test")
    connectProperties.put("password", "test")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    model.freqItemsets.write.mode(SaveMode.Overwrite).jdbc(url,"S_FP_FRE01_1",connectProperties)
    model.associationRules.write.mode(SaveMode.Overwrite).jdbc(url,"S_FP_ASS01_1",connectProperties)

    spark.stop()
  }
}
