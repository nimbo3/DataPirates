package in.nimbo;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.example.hbasecontext.JavaHBaseStreamingBulkPutExample;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import java.util.List;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Test Spark").getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);
        distData.foreach(i -> {
            System.out.println(i);
        });
        Integer integer = distData.reduce(Integer::sum);
        System.err.println("---------------------------------------------------------------------------------------------");
        System.err.println(integer);
        System.err.println("---------------------------------------------------------------------------------------------");
        spark.stop();
    }
}
