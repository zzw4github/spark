package spark.example;



import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MachineLearning {
	public static void main(String[] args) {

		SparkSession jsql = SparkSession
			      .builder()
			      .appName("MachineLearning")
			      .master("local[1]")
			      .getOrCreate();
		 JavaSparkContext jsc = new JavaSparkContext(jsql.sparkContext());
		// Every record of this DataFrame contains the label and
		// features represented by a vector.
		StructType schema = new StructType(new StructField[]{
		  new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
		  new StructField("features", new VectorUDT(), false, Metadata.empty()),
		});

		Dataset<Row> df = jsql.createDataFrame(data, schema);

		// Set parameters for the algorithm.
		// Here, we limit the number of iterations to 10.
		LogisticRegression lr = new LogisticRegression().setMaxIter(10);

		// Fit the model to the data.
		LogisticRegressionModel model = lr.fit(df);

		// Inspect the model: get the feature weights.
//		Vector weights =  model.weights();

		// Given a dataset, predict each point's label, and show the results.
		model.transform(df).show();


	}
}
