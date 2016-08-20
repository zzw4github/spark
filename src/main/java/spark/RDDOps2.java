package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
/**
 * 引用外部文件系统的数据集（HDFS）创建RDD 
 *  外部类定义函数传给spark
 * @author 汤高
 *
 */
public class RDDOps2 {
    // 完成对所有行的长度求和
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.set("spark.testing.memory", "2147480000"); // 因为jvm无法获得足够的资源
        JavaSparkContext sc = new JavaSparkContext("local", "First Spark App", conf);
        System.out.println(sc);


        //通过hdfs上的文件定义一个RDD 这个数据暂时还没有加载到内存，也没有在上面执行动作,lines仅仅指向这个文件
        JavaRDD<String> lines = sc.textFile("hdfs://master:9000/user/zzw/input/hdfs-site.xml");
        //定义lineLengths作为Map转换的结果 由于惰性，不会立即计算lineLengths
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());


        //运行reduce  这是一个动作action  这时候，spark才将计算拆分成不同的task，
                //并运行在独立的机器上，每台机器运行他自己的map部分和本地的reducation，并返回结果集给去驱动程序
        int totalLength = lineLengths.reduce(new Sum());

        System.out.println("总长度"+totalLength);
        // 为了以后复用 持久化到内存...
        lineLengths.persist(StorageLevel.MEMORY_ONLY());

    }
    //定义map函数
    //第一个参数为传入的内容，第二个参数为函数操作完后返回的结果类型
    static class GetLength implements Function<String, Integer> {
        public Integer call(String s) {
            return s.length();
        }
    }
    //定义reduce函数 
    //第一个参数为内容，第三个参数为函数操作完后返回的结果类型
    static class Sum implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) {
            return a + b;
        }
    }
}
