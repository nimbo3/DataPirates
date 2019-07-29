package in.nimbo;
import org.apache.spark.sql.SparkSession;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Test Spark").getOrCreate();
        System.out.println("this is a message to all salves!!!");
        spark.stop();
    }
}
