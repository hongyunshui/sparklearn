import org.apache.spark.sql.SparkSession

/**
  * Created by hys on 2018-09-08.
  */
object Demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()

  }
}
