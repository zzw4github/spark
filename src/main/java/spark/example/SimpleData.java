package spark.example;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SimpleData {
	public static void main(String[] args) {

		SparkSession spark = SparkSession
			      .builder()
			      .appName("JavaWordCount")
			      .master("local[1]")
			      .getOrCreate();

			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		// Creates a DataFrame based on a table named "people"
		// stored in a MySQL database.
		String url =
		  "jdbc:mysql://localhost:3306/test?user=root&password=zzw";
		Dataset<Row> df = spark
		  .read()
		  .format("jdbc")
		  .option("url", url)
		  .option("dbtable", "people")
		  .load();

		// Looks the schema of this DataFrame.
		df.printSchema();

		// Counts people by age
		Dataset<Row> countsByAge = df.groupBy("age").count();
		countsByAge.show();

		// Saves countsByAge to S3 in the JSON format.
		//countsByAge.write().format("json").save("s3a://...");
		countsByAge.write().format("json").save("file:///usr/hadoop-2.7.2/input/mysql_people");
		jsc.close();

	}
}
