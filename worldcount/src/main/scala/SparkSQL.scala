import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

//DataFrame是spark1.3.0版本提出来的，spark1.6.0版本引入了DateSet，在spark2.0版本中，DataFrame和DataSet合并为DataSet。
//我们通常将Datesets[row]成为DateFrames
//Spark 2.0之后提供了对Hive功能的内置支持，包括使用HiveQL编写查询，访问Hive UDF以及从Hive表读取数据的功能。
object SparkSQL {
  case class Person(name:String,age:Long)
  def main(args: Array[String]): Unit = {
    //创建基本的SparkSession，Spark中所有功能入口点都是SparkSession类
    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()
    DataFrameBasice(spark)    //DataFrame基础操作
    //DatasetCreation(spark)    //根据RDD创建数据集
    //InferSchemaE(spark)       //将现有的RDD转化成数据集的两种办法 1.反射推断模式
    //ProgrammaticSchema(spark)  //将现有的RDD转化成数据集的两种办法 2.以编程方式指定架构
  }

  private def DataFrameBasice(spark:SparkSession): Unit = { //导入隐式转换

    val df:DataFrame = spark.read.json("H:\\软件\\spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    df.write.save("D:\\b")//使用.write.save()进行保存其中的path是一个路径 不指定文件名字  比如 d:\\a d盘下面a文件夹 且a文件夹不存在
    //基于json创建的DateFrame
    //DataFrame操作
    df.show()// 打印数据集内容 括号里面有一个Int类型的参数 可写可不写 指的是返回多少条数据 不写默认20条
    df.printSchema()//打印数据集结构
    df.groupBy("age").count().show()//按照age分组并计数
    df.select("name","age").show()//show name age 列
  //  df.select($"name",$"age"+1).show()//show name age 列并将age列值+1
  //  df.select($"age" > 21).show()//show 数据集中age大于21的数据

    df.registerTempTable("people") //将DataFrame对象形成表
    spark.sql("select * from people").show()  //编程式运行查询
    df.createOrReplaceTempView("p") //创建临时视图
    df.createOrReplaceGlobalTempView("people")//创建全局临时视图
    spark.sql("select * from global_temp.people").show()//全局临时视图与系统保存的数据库global_temp绑定,使用限定名称引用它
    spark.newSession().sql("select * from global_temp.people").show()//全局临时视图是跨会话的
    //Spark SQL中的临时视图是会话范围的，如果创建它的会话终止，将会消失。 如果希望在所有会话之间共享一个临时视图并保持活动状态，直到Spark应用程序终止，则可以创
    //建一个全局临时视图。
    //spark.stop() Spark应用程序终止 所有会话消失

  }

  private def DatasetCreation(spark:SparkSession): Unit = {
    import spark.implicits._

    val caseClassDS = Seq(Person("aa",12)).toDS()//创建数据集，使用编码器来序列化对象，将对象转换为字节 为case类创建编码器
    caseClassDS.show()

    val pd = Seq(1,2,3).toDS()//对于最常见的类型//编码器是通过导入spark.implicits._自动提供
    pd.map(_ + 1).collect()//返回数组（2,3,4）
    pd.show()

    val path = "H:\\软件\\spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json"
    val pDS = spark.read.json(path).as[Person]
    pDS.show()//可以通过提供类将数据aframes转换为数据集。映射将按名称进行
  }

  private def InferSchemaE (spark:SparkSession):Unit ={
    import spark.implicits._ //导入隐式转换
    //将现有的RDD转换为数据集的两种方法
    //第一种：使用反射推断模式
    val pp = spark.sparkContext.textFile("H:\\软件\\spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
      .map(_.split(","))
      .map(at => Person(at(0),at(1).trim.toInt))
      .toDF()
    pp.createOrReplaceGlobalTempView("pp")
    spark.sql("select * from global_temp.pp").show()
    val tt = spark.sql("select * from global_temp.pp where age between 13 and 19")//查询13到19的数据
    tt.show()
    tt.map(te => "Name:" + te(0)).show()//结果中的行的列可以由字段索引
    tt.map(te => "Name:" + te.getAs("name")).show()//或者按照字段名称

    //没有预先定义的数据集编码器[Map[K,V]]，明确定义。
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    //基本类型和case类也可以定义为
    //implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    val te = tt.map(te => te.getValuesMap[Any](List("name","age"))).collect()
    // Array（Map（“name” - >“Justin”，“age” - > 19））
  }

  private def ProgrammaticSchema(spark:SparkSession):Unit = {//以编程方式推断架构
//    如果不能提前定义case类(例如，记录的结构被编码在字符串中，或者文本数据集将被解析，字段将被不同的用户以不同的方式投影)，那么可以通过三个步骤以编程的方式创建DataFrame。
//      1.从原始RDD创建行dd;
//      2.创建由与步骤1中创建的RDD中的行结构匹配的StructType表示的模式。
//      3.通过SparkSession提供的createDataFrame方法将模式应用到行的RDD。

    import spark.implicits._
    //根据一个文本文件创建一个RDD
    val rdd = spark.sparkContext.textFile("H:\\软件\\spark\\spark-2.2.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
    //模式被编码在一个字符串中
    val schemaString = "name age"
    //基于模式字符串生成模式
    val file = schemaString.split(" ")
      .map(e => StructField(e, StringType, nullable = true))
    val schema = StructType(file)
    //将rdd 转换成行
    val rowrdd = rdd.map(e => e.split(","))
      .map(at => Row(at(0),at(1).trim))
    //将模式应用到RDD
    val a = spark.createDataFrame(rowrdd,schema)

    a.createOrReplaceTempView("a")
    val b = spark.sql("select * from a")
    // SQL查询的结果是DataFrames，并支持所有正常的RDD操作
    //可以通过字段索引或字段名访问结果中的行列
    b.map(e => "Name:" + e(0)).show()
  }
}