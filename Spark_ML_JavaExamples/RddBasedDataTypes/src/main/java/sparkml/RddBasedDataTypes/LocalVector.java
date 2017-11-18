package sparkml.RddBasedDataTypes;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * @author Dhinesh Raja M
 *
 */
/*
 * Local vectors in MLlibs are  0 based indices and with integer and double type values only
 */
public class LocalVector 
{
    public static void main( String[] args )
    {
        Vector sparseVector = Vectors.sparse(3, new int[] {1,2},new double[] {2.0,8.0});
        System.out.println("The Sparse Vector : " + sparseVector.toDense());
        
        Vector denseVector = Vectors.dense(2.0,3.0,4.0);
        System.out.println("The Dense Vector: " + denseVector);
    }
}
