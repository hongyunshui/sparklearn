/**
 * Created by hys on 2018-11-05.
 */

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
 * 本地测试的word count程序
 *
 */
public class WordCount {
    public static void main(String[] args) {
        //编写spark应用程序
        //本地执行是可以直接在main()方法中执行的
        System.out.println("Hello World");
        //第一步：创建SparkConf对象，设置Spark应用的配置信息
        //使用setMaster()可以设置Spark应用程序要连接的Spark集群的master节点的url，如果设置为local，代表本地运行
        SparkConf conf = new SparkConf()
                .setAppName("WorkCountLocal")
                .setMaster("local");

        //第二步：创建JavaSparkContext对象
        //在Spark中，SparkContext是Spark所有功能的一个入口，无论是用Java、Scala、，甚至是Python编写都需要有一个
            // SparkContext，它的主要作用包括初始化Spark应用程序所需要的一些核心组件，包括调度器（DAGSchedule、
            // TaskScheduler），还会去到SparkMaster节点上进行注册，等等，总之SparkContext是Spark应用中，比较重要的一个
            // 对象。
        // 同时在Spark中编写不同的Spark应用程序，要使用不同的SparkContext：
            // 如果使用Scala，使用的就是原生的Spark Context对象
            //如果使用Java，就是JavaSparkContext对象
            //如果使用SparkSQL，就是SQLContext或者HiveContext
            //以此类推
        JavaSparkContext sc = new JavaSparkContext(conf);

        //第三步：要针对输入源（hdfs文件、本地文件，等等），创建一个初始化RDD。输入源中的数据会被打散分配到RDD的每个
        // partition中，从而形成一个初始化的分布式的数据集。在这里我们使用textFile()方法读取本地文件创建一个RDD。
        //Java中创建的普通RDD，都叫做JavaRDD。
        //RDD中有元素的概念，如果是hdfs或者本地文件，创建的RDD的每一个元素就相当于是文件里的一行。

        JavaRDD<String> lines = sc.textFile("E:\\files\\LearnFiles\\Spark\\filesCreateByMyself\\worldCountTest.txt");

        //第四步：对初始RDD进行transformation操作，也就是一些计算操作，通常操作会通过创建function，并配合RDD的map、
        // flatMap等算子来执行function，如果比较简单则指定Function的匿名内部类，如果比较复杂，则会单独创建一个类，
        // 作为实现这个function接口的类。

        //先将每一行拆分成单个的单词
        //FlatMapFunction,有两个泛型参数，分别代表了输入和输出类型，我们这里输入时String，输出也是String
        //FlatMap算子的作用就是将RDD的一个元素拆分成一个或者多个元素
//        JavaRDD<String> w = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String string) throws Exception {
//                String line = string;
//                return Arrays.asList(line.split);
//            }
//        });

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                List<String> list = new ArrayList<String>();
                String[] sp = s.split(" ");
                for(int j = 0 ;j < sp.length; j++){
                    list.add(sp[j]);
                };
                Iterator<String> ill = list.iterator();
                return ill;
            }
        });

        // 接下来将每一个单词映射为（单词，1）的格式,这是一个tuple
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //接下来使用reduce操作统计单词出现的次数
        //reduceByKey操作相当于把key相同的value值累加，最后以tuple的形式返回JavaRDD中的key及对应的value累加值
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }));
//        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });

        //上面的算子（transformation）计算出了我们想要的结果，但是Spark如果要执行程序，还需要action操作。
        // 创建action执行程序
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._2 + "次"+ ":\t" + wordCount._1 );

            }
        });
        // 关闭sc
        sc.close();
    }
}
