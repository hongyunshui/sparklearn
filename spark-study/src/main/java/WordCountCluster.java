import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 练习把spark程序提交到集群上执行
 * Created by hys on 2018-11-05.
 */
public class WordCountCluster {
    public static void main(String[] args){
        // 1.将文本文件上传到hdfs
        // 2.使用在pom.xml文件中配置的maven插件，对spark工程进行打包
        // 3.将打包后的spark工程jar包上传到机器执行
        // 4.编写spark-submit 脚本
        // 5.执行spark-submit 脚本，提交spark应用到集群执行
        wcc();
    }
    public static void wcc(){

        //第一步：创建SparkConf对象，设置Spark应用的配置信息
        SparkConf conf = new SparkConf()
                .setAppName("WorkCountCluster");
        //第二步：创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //第三步：要针对输入源（hdfs文件、本地文件，等等），创建一个初始化RDD
        JavaRDD<String> lines = sc.textFile("hdfs://hadoop-1:9000/wordcounttest/worldCountTest.txt");
        //第四步：对初始RDD进行transformation操作，也就是一些计算操作
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] sp = s.split(" ");
                for(int i = 0 ;i < sp.length; i++){
                    list.add(sp[i]);
                };
                Iterator<String> il = list.iterator();
                return il;
            }
        });
        // 接下来将每一个单词映射为（单词，1）的格式,这是一个tuple
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        //接下来使用reduce操作统计单词出现的次数
        final JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        // 创建action执行程序
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._2 + "次"+ ":\t" + wordCount._1 );
            }
        });
        // 关闭sc
        sc.close();
    }
}

