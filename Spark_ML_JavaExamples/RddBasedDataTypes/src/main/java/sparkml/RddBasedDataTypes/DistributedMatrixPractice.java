package sparkml.RddBasedDataTypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.QRDecomposition;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

/*
 * 4 types of distributed matrix re implemented in Spark so far
 * 1. Row Matrix
 * 2. Indexed Row Matrix
 * 3. Coordinate Matrix
 * 4. Block Matrix
 */
public class DistributedMatrixPractice {
	public static void main(String[] args) {

		/*
		 * 1. Row Matrix
		 *  “row-oriented distributed matrix without meaningful row indices” — RDD of sequence of Vector without rows indices
		 */
		SparkConf conf = new SparkConf().setAppName("DistributedMatrix").setMaster("local[1]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		List<Vector> vectorList = new ArrayList<>();
		vectorList.add(Vectors.dense(1.0,3.0,4.0));
		vectorList.add(Vectors.dense(2.0,7.0,9.0));
		vectorList.add(Vectors.dense(4.0,2.0,3.0));
		JavaRDD<Vector> rowMatData = jsc.parallelize(vectorList);
		RowMatrix rMatrix = new RowMatrix(rowMatData.rdd());
		System.out.println("Number of Column in Row Matrix: " + rMatrix.numCols());
		System.out.println("Number of Row in Row Matrix: " + rMatrix.numRows());
		QRDecomposition<RowMatrix, Matrix> qr = rMatrix.tallSkinnyQR(true);
		System.out.println("QR decomposed RowMatrix size: " + qr.Q().numRows() + " X " + qr.Q().numCols());
		System.out.println("QR decomposed Matrix size: " + qr.R().numRows() + " X " + qr.R().numCols());
		System.out.println("Row Matrix: " + rMatrix.tallSkinnyQR(true));
		
		
		
		/*
		 * 2.Indexed Row Matrix
		 * IndexedRowMatrix: like a RowMatrix but with meaningful row indices
		 */
		List<IndexedRow> indexRowList = new ArrayList<>();
		indexRowList.add(new IndexedRow(0,Vectors.dense(1.0,3.0)));
		indexRowList.add(new IndexedRow(2, Vectors.dense(3.0,2.0)));
		indexRowList.add(new IndexedRow(1, Vectors.dense(5.0,6.0)));
		JavaRDD<IndexedRow> indexRowMatData = jsc.parallelize(indexRowList);
		IndexedRowMatrix iRMat = new IndexedRowMatrix(indexRowMatData.rdd());
		System.out.println("Number of rows in the Indexed row matrix : " + iRMat.numRows());
		System.out.println("Number of columns in the Indexed row matrix: " + iRMat.numCols());
	
		iRMat.rows().toJavaRDD().foreach(new VoidFunction<IndexedRow>() {
			
			@Override
			public void call(IndexedRow arg0) throws Exception {
				
				System.out.println(arg0.vector().toDense());
				
			}
		});
		
		
		RowMatrix conMat =  iRMat.toRowMatrix();
		
		
		
		/*
		 * 3. Coordinate Matrix
		 * A CoordinateMatrix is a distributed matrix backed by an RDD of its entries
		 * CoordinateMatrix: elements values are explicit defined by using IndexedRow(row_index, col_index, value)
		 */
		List<MatrixEntry> matEntry = new ArrayList<>();
		matEntry.add(new MatrixEntry(0, 0, 1.0));
		matEntry.add(new MatrixEntry(0, 1, 3.0));
		matEntry.add(new MatrixEntry(1, 0, 6.0));
		matEntry.add(new MatrixEntry(1, 1, 9.0));
		JavaRDD<MatrixEntry> jMatEntry = jsc.parallelize(matEntry);
		CoordinateMatrix coMat = new CoordinateMatrix(jMatEntry.rdd());
		System.out.println("Number of rows in Coordinate MAtrix: " + coMat.numRows());
		System.out.println("Number of columns in Coordinate Matrix: " + coMat.numCols());
		
		
		/*
		 * 4. Block Matrix
		 * BlockMatrix: included set of matrix block (row_index, col_index, matrix)
		 */
		
		BlockMatrix bMat = coMat.toBlockMatrix().cache();
		bMat.validate();
		
		BlockMatrix bbtrans = bMat.transpose().multiply(bMat);
	}
}
