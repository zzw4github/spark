package spark.example;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TextSearch {
	public static void main(String[] args) {
		SparkSession spark = SparkSession
			      .builder()
			      .appName("JavaWordCount1")
			      .master("local[1]")
			      .getOrCreate();

			    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

		// Creates a DataFrame having a single column named "line"
		//JavaRDD<String> textFile = jsc.textFile("hdfs://...");
		JavaRDD<String> textFile = jsc.textFile("file:///usr/hadoop-2.7.2/input/mysql1.txt");
		JavaRDD<Row> rowRDD = textFile.map(
		  new Function<String, Row>() {
		    public Row call(String line) throws Exception {
		      return RowFactory.create(line);
		    }
		  });
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));
		StructType schema = DataTypes.createStructType(fields);
		Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

		Dataset<Row> errors = df.filter(df.col("line").like("%ERROR%"));
		// Counts all the errors
		System.out.println(errors.count()+ " ---- ERROR");
		// Counts errors mentioning MySQL
		Dataset<Row> mySQL = df.filter(df.col("line").like("%MySQL%"));
		System.out.println(mySQL.count() + "  ----MySQL");
		// Fetches the MySQL errors as an array of strings
		errors.filter(df.col("line").like("%MySQL%")).collect();
jsc.close();

	}
}
