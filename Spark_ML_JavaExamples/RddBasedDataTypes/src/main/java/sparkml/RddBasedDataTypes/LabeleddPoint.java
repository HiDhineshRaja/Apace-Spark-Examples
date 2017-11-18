package sparkml.RddBasedDataTypes;

import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

public class LabeleddPoint {
	public static void main(String[] args) {
		LabeledPoint pos = new LabeledPoint(1.0	, Vectors.dense(1.0,4.0,8.0));
		System.out.println("Positive Labeled Point Dense Vector: " + pos);
		LabeledPoint neg = new LabeledPoint(0.0, Vectors.dense(4.0,9.0,5.0));
		System.out.println("Negative Labeled Point Dense Vector: " + neg);
		
		LabeledPoint posSparse = new LabeledPoint(1.0, Vectors.sparse(4, new int[] {2,0}, new double[] {1.0,6.0}));
		System.out.println("Positive Labeled Point Sparse vector: " + posSparse.features().toDense());
		LabeledPoint negSparse = new LabeledPoint(0.0, Vectors.sparse(4, new int[] {1}, new double[] {3.0}));
		System.out.println("Negative Labeled Point Sparse vector: " + negSparse.features().toDense());
	}
}
