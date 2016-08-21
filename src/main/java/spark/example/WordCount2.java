package spark.example;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;



public class WordCount2 {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
			      .builder()
			      .appName("spark://127.0.0.1:8080")
			      .master("local[4]")
			      .getOrCreate();

			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		JavaRDD<String> textFile = jsc.textFile("file:///usr/spark-2.0.0-bin-hadoop2.7/README.md");
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split(" ")).iterator();
			}
		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		});
		counts.saveAsTextFile("/usr/spark-2.0.0-bin-hadoop2.7/README.WordCount.SPARK.STANDALONE");
		jsc.close();
	}
}
