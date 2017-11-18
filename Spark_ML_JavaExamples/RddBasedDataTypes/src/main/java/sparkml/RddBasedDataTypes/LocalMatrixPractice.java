package sparkml.RddBasedDataTypes;

import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;

public class LocalMatrixPractice {
	public static void main(String[] args) {
		Matrix denseMatrix = Matrices.dense(4, 2, new double[] {1.0,3.0,2.0,4.0,2.0,1.0,8.0,9.0});
		System.out.println("The dense Matrix: " + denseMatrix);
		
		Matrix sparseMatrix = Matrices.sparse(4, 2, new int[] {0,4,8}, new int[] {0,1,2,3,0,1,2,3}, new double[] {1.0,3.0,2.0,4.0,2.0,1.0,8.0,9.0 });
		System.out.println("The Sparse MAtrix: " + sparseMatrix);
	}
}
