package sparkml.RddBasedDataTypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;

/*
 * 
 * LIBSVM is a text format in which each line represents a labeled sparse feature vector using the following format:
 * 
 * label index1:value1 index2:value2 ...
 */
public class SparseData {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("SparseData").setMaster("local[1]");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		/*
		 * Save the data in LIBSVM format
		 */
		LabeledPoint posSparse = new LabeledPoint(1.0, Vectors.sparse(4, new int[] {1,2}, new double[] {1.0,6.0}));
		LabeledPoint negSparse = new LabeledPoint(0.0, Vectors.sparse(4, new int[] {1}, new double[] {3.0}));
		List<LabeledPoint> labelList = new ArrayList<>();
		labelList.add(posSparse);
		labelList.add(negSparse);
		RDD<LabeledPoint> labelRdd = jsc.parallelize(labelList).rdd();
		MLUtils.saveAsLibSVMFile(labelRdd, "F:\\Dhinesh_Raja\\notes\\Apache_Spark\\Spark_ML\\RddBasedDataTypes\\src\\main\\resources\\sample_libsvm_data1.txt");
		
		
		/*
		 * Loading the data saved in the LIBSVM format
		 */
		JavaRDD<LabeledPoint> example = MLUtils.loadLibSVMFile(jsc.sc(), "F:\\Dhinesh_Raja\\notes\\Apache_Spark\\Spark_ML\\RddBasedDataTypes\\src\\main\\resources\\sample_libsvm_data1.txt").toJavaRDD();
		System.out.println(example.count());
		System.out.println(example.collect().get(0).features().toDense());
		List<LabeledPoint> data = example.collect();
		for (LabeledPoint labeledPoint : data) {
			System.out.print("Label: " + labeledPoint.label() + " ===> ");
			System.out.println("Features: " + labeledPoint.features().toJson());
		}
		
		JavaRDD<LabeledPoint> example1 = MLUtils.loadLibSVMFile(jsc.sc(), "F:\\Dhinesh_Raja\\notes\\Apache_Spark\\Spark_ML\\RddBasedDataTypes\\src\\main\\resources\\sample_libsvm_data.txt").toJavaRDD();
		System.out.println(example1.count());
		System.out.println(example1.collect().get(0).features().toDense());
		List<LabeledPoint> data1 = example.collect();
		for (LabeledPoint labeledPoint : data1) {
			System.out.print("Label: " + labeledPoint.label() + " ===> ");
			System.out.println("Features: " + labeledPoint.features().toJson());
		}
		
	}
}
