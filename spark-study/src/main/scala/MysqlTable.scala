//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by hys on 2018-10-30.
  */
object MysqlTable {
  def apply: MysqlTable = new MysqlTable()
  Class.forName("oracle.jdbc.driver.OracleDriver")
  val spark = SparkSession
    .builder()
    .appName("Test001")
    .master("local")
    .getOrCreate()
  //val mt = MysqlTable
  def main(args: Array[String]): Unit = {
    def mysqlTable(sqlContext: SparkSession, tableName: String): DataFrame = {
      val frame: DataFrame = sqlContext.read.format("jdbc")
        .option("url", "jdbc:mysql://192.168.1.3:3306/test_01")
        .option("user", "xulin")
        .option("password", "xulin")
        .option("dbtable", tableName)
        .option("driver", "com.mysql.jdbc.Driver")
        .load()
      frame
    }

    //读取mysql jdbc中的目标表：教师信息表、学生信息表、课程信息表、成绩表
    val teacherTable = mysqlTable(spark, "Teacher") //.withColumnRenamed("name","teachername")
    val studentTable = mysqlTable(spark, "Student") //.withColumnRenamed("name","studentname")
    val courseTable = mysqlTable(spark, "Course") //.withColumnRenamed("name","coursename")
    val scoreTable = mysqlTable(spark, "Sc")

    //    scoreTable.join(studentTable,"s#").show()
    //     scoreTable.select("","","")
    teacherTable.show()
    studentTable.show()

    sqlTest(spark)


    /**
      * sql测试练习实现
      *
      * @param spark
      */
    def sqlTest(spark: SparkSession) = {
      val url = "jdbc:mysql://192.168.1.3:3306/test_01"
      // 获得得分表score（S#,C#,score）
      val scoreDF = spark.read.format("jdbc").options(Map("url" -> url,
        "user" -> "xulin",
        "password" -> "xulin",
        "dbtable" -> "Sc")).load()
      scoreDF.createOrReplaceTempView("scc")
      // 获得学生表 student(s#,Sname,Sage,Ssex)
      val studentDF = spark.read.format("jdbc").options(Map("url" -> url,
        "user" -> "xulin",
        "password" -> "xulin",
        "dbtable" -> "Student")).load()
      studentDF.createOrReplaceTempView("stt")
      // 获得教师表teacher(T#,Tname)
      val teacherDF = spark.read.format("jdbc").options(Map("url" -> url,
        "user" -> "xulin",
        "password" -> "xulin",
        "dbtable" -> "Teacher")).load()
      teacherDF.withColumnRenamed("t#", "tnum").createOrReplaceTempView("tch")
      // 获得课程表Course(C#,Cname,T#)
      val courseDF = spark.read.format("jdbc").options(Map("url" -> url,
        "user" -> "xulin",
        "password" -> "xulin",
        "dbtable" -> "Course")).load()
      courseDF.createOrReplaceTempView("coo")

      spark.sql(" select * from scc ").show(100)
      spark.sql("select * from stt").show(100)
      spark.sql("select * from tch").show(100)
      spark.sql("select * from coo").show(100)
      spark.sql("select * from tch t where t.tnum = 2").show(5)
    }
  }
}

class MysqlTable(){





}
