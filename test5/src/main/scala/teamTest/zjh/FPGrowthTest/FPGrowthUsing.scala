package teamTest.zjh.FPGrowthTest

/**
  * Created by hys on 2018-11-22.
  */
import org.apache.spark.mllib.fpm.FPGrowth

/**
  * Created by Administrator on 2016/10/24.
  */
object FPGrowthUsing {
  def apply(): FPGrowthUsing = new FPGrowthUsing()

  def main(args:Array[String]){
    val fpp = FPGrowthUsing()
    fpp.fpGrowthTest()
  }
}
class FPGrowthUsing(){
  def fpGrowthTest() ={
    //设置参数
    //最小支持度
    val minSupport=0.001
    //最小置信度
    val minConfidence=0.5
    //数据分区
    val numPartitions=2

    //取出数据
//    val data = sc.textFile("E:\\files\\LearnFiles\\DataAnalysis\\modelData\\")
    val data = myTools.OracleTools().get_itemSet_RDD("FP_CARDID_STATIONS_HYS", "GROUP_STATION")
    println("*************已经获取初始数据集**************")

    //把数据通过逗号分割
    val transactions=data.map(x=>x.getString(0).split(","))
    transactions.cache()
    //创建一个FPGrowth的算法实列
    val fpg = new FPGrowth()
    //设置训练时候的最小支持度和数据分区
    fpg.setMinSupport(minSupport)
    fpg.setNumPartitions(numPartitions)

    //把数据带入算法中
    val model = fpg.run(transactions)

    //查看所有的频繁项集，并且列出它出现的次数
    println("************所有频繁项集及次数****************")
    model.freqItemsets.collect().foreach(itemset=>{
      println( itemset.items.mkString("[", ",", "]")+","+itemset.freq)
    })

    //通过置信度筛选出推荐规则则
    //antecedent表示前项
    //consequent表示后项
    //confidence表示规则的置信度
    //这里可以把规则写入到Mysql数据库中，以后使用来做推荐
    //如果规则过多就把规则写入redis，这里就可以直接从内存中读取了，我选择的方式是写入Mysql，然后再把推荐清单写入redis
    println("********产生的规则及置信度**********")
    model.generateAssociationRules(minConfidence).collect().foreach(rule=>{
      println(rule.antecedent.mkString(",")+"-->"+
        rule.consequent.mkString(",")+"-->"+ rule.confidence)
    })
    //查看规则生成的数量
    println("****************规则个数为：********************")
    println(model.generateAssociationRules(minConfidence).collect().length)

    //并且所有的规则产生的推荐，后项只有1个，相同的前项产生不同的推荐结果是不同的行
    //不同的规则可能会产生同一个推荐结果，所以样本数据过规则的时候需要去重

    //写入数据库
//    val connectProperties = new Properties()
//    connectProperties.put("user", "test")
//    connectProperties.put("password", "test")
//    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
//    c.write.mode(SaveMode.Overwrite).jdbc(url,"up_down_cardid1",connectProperties)
  }
}