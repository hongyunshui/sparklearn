package teamTest.fxx

import java.sql.Types
import java.util.Properties
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, DataTypes, MetadataBuilder}

/**
  * Created by hys on 2018-11-26.
  */
object FP_CREATE_TABLE1 {

  def oracleInit(): Unit = {
    val dialect = new JdbcDialect(){ //判断是否为oracle库
    override def canHandle (url: String): Boolean = url.startsWith("jdbc:oracle") //用于读取Oracle数据库时数据类型的转换
    override def getCatalystType (sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
      if (sqlType == Types.DATE && typeName == "DATE" && size == 0) return Option.apply(DataTypes.TimestampType)
      Option.empty
    }
    }
    JdbcDialects.registerDialect(dialect)
  }
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val url = "jdbc:oracle:thin:test/test@//192.168.1.238:1521/orcl"
  def oracleTable(spark: SparkSession, url: String, tableName: String): DataFrame = {
    val oracleDf = spark.read.format("jdbc").options(Map("url" -> url,
      "user" -> "test",
      "password" -> "test",
      "dbtable" -> tableName)).load()
    oracleDf
  }
  def spark_init(APPName: String, flag:String): SparkSession = {
    if (flag == "true") {
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
  def main(args: Array[String]): Unit = {
//    var spark:SparkSession = null
//    if(args(0) == "false") {
//      // true 为集群模式，需要打包在集群上跑，false为测试，本地跑
//      spark = spark_init("XULINV_CARD_INFO", local = false)
//    } else if(args(0) == "true") {
//      spark = spark_init("XULINV_CARD_INFO", local = true)
//    } else{
//      println("*******请输入正确的参数********")
//    }
    val spark = spark_init("XULINV_STATION_REACH_TIME_INFO", "false")
    // 这是读取
    val cardlogDF: DataFrame = oracleTable(spark, url, "XL_CARD_LOG_02")
    // 这是注册临时表
    cardlogDF.createOrReplaceTempView("XL_CARD_LOG_02")

    val hisDF: DataFrame = oracleTable(spark, url, "XULINV_STATION_REACH_TIME_INFO")
    hisDF.createOrReplaceTempView("XULINV_STATION_REACH_TIME_INFO")
    val sql_rel1 = "select bus_no,line_no,reach_time,station_label_no,station_id,station_name from XULINV_STATION_REACH_TIME_INFO"
    val sql_rel2 = "select cardid,txndatetime,buscode from XL_CARD_LOG_02"
    val sql03 =
      """
        select
        |re2.cardid,
        |re2.txndatetime,
        |re1.is_up_down,
        |re2.buscode,
        |re1.line_no,
        |re1.reach_time,
        |re1.station_label_no,
        |re1.station_id,
        |re1.station_name
        |from
        |XULINV_STATION_REACH_TIME_INFO re1,
        |XL_CARD_LOG_01 re2
        |where
        |re2.buscode=re1.bus_no
        |and to_date(re2.txndatetime, 'yyyy-mm-dd hh24:mi:ss') between re1.reach_time
        |and(re1.reach_time + interval '1' MINUTE)
        |and re1.LINE_NO like '%5%'
      """.stripMargin
    //      .stripMargin
    println("到站时间与刷卡时间相差一分钟内的数据结果：")
    val c = spark.sql(sql03)
    val rel1 = spark.sql(sql_rel1)
    rel1.limit(5).show()
    val rel2 = spark.sql(sql_rel2)
    rel2.limit(5).show()
    println("*************通过BUSNO_NO进行表关联****************")
    val rel_join = rel1.join(rel2, rel1("BUS_NO") === rel2("buscode"))
    println("*************显示join后的情况*************")
    rel_join.limit(5).show()
    // TODO
    val card_info_hys = rel_join.filter(line => line.length > 0)
    println("**********SparkSQL DATE***************")
//    rel1.filter("reach_time").show()
    println("******CCCCCCCCCCCCCCCCC********")
    c.limit(5).show()
    c.createOrReplaceTempView("up_down_cardid")
    // 把时间戳转换成date类型
    c.withColumn("dateColumn",c("txndatetime").cast(DateType))
    val connectProperties = new Properties()
    connectProperties.put("user", "test")
    connectProperties.put("password", "test")
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    c.write.mode(SaveMode.Overwrite).jdbc(url,"up_down_cardid1",connectProperties)
    spark.stop()
  }
}
