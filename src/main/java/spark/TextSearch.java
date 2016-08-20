package spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.spark_project.jetty.util.Fields;
import org.apache.spark.sql.types.DataTypes;

public class TextSearch {
	public static void main(String[] args) {
	// Creates a DataFrame having a single column named "line"
	SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[1]");
    JavaSparkContext sc = new JavaSparkContext(conf);
	JavaRDD<String> textFile = sc.textFile("hdfs://localhost:9000/user/zzw/input/mysql.txt");
	JavaRDD<Row> rowRDD=textFile.map(
			new Function<String,Row>(){
				public Row call(String line)throws Exception{
					return RowFactory.create(line);
					}
				});
	List<StructField> fields = new ArrayList<StructField>();
	
	fields.add(DataTypes.createStructField("line", DataTypes.StringType, true));

	StructType schema = DataTypes.createStructType(fields);
	
	SparkSession sqlContext = SparkSession.builder()
			.master("local[1]")
			.appName("TextSearch")
			.config("", "")
			.getOrCreate();
	Dataset df = sqlContext.createDataFrame(rowRDD, schema);
	Dataset errors = df.filter(df.col("line").like("%ERROR%"));
	// Counts all the errors
	errors.count();
	// Counts errors mentioning MySQL
	System.out.println(errors.filter(df.col("line").like("%MySQL%")).count());
	// Fetches the MySQL errors as an array of strings
	Object collect =  errors.filter(df.col("line").like("%MySQL%")).collect();
	
	
	sc.close();
}

}
