package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class MyDataset {
	public static void main(String[] args) {
		SparkSession sqlContext = SparkSession.builder()
				.master("local[1]")
				.appName("TextSearch")
				.config("", "")
				.getOrCreate();
//		http://parquet.apache.org/
		Dataset<Person> people = sqlContext.read().parquet("hdfs://localhost:9000/user/zzw/input/people.csv").as(Encoders.bean(Person.class));
//		Dataset<String> names = people.map((Person p) -> p.name,Encoders.STRING());
		System.out.println(people.col("pwd").plus(10));  // in Java
		
	}
}
