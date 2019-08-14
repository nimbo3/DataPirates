package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;


public class App {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseTableName = config.getString("hbase.table.name");
        String hbaseColumnFamily = config.getString("hbase.column.family");
        String sparkExecutorCores = config.getString("spark.executor.cores");
        String sparkExecutorMemory = config.getString("spark.executor.memory");

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);
        hbaseConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "500");

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.executor.cores", sparkExecutorCores)
                .set("spark.executor.memory", sparkExecutorMemory);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Result> hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class).values();

        JavaRDD<Cell> hbaseCells = hbaseRDD.flatMap(result -> result.listCells().iterator());

        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairRDD = hbaseCells.mapToPair(cell -> {
            String source = new String(cell.getRowArray());
            String destination = new String(cell.getValueArray());
            String sourceDomain = getDomain(source);
            String destinationDomain = getDomain(destination);
            // Todo : Handle Malformed URL Exception
            Tuple2<String, String> domainPair = new Tuple2<>(sourceDomain, destinationDomain);
            return new Tuple2<>(domainPair, 1);
        });
        // Todo : Difference between getRowArray & clone

        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairWeightRDD = domainToDomainPairRDD
                .reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        domainToDomainPairWeightRDD.foreach((VoidFunction<Tuple2<Tuple2<String, String>, Integer>>) tuple2IntegerTuple2 -> {
            System.out.println(String.format("%s -> %s : %d", tuple2IntegerTuple2._1._1, tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
        });

        // Todo : Store data in hbase

        sparkContext.close();

    }

    public static String getDomain(String url) throws MalformedURLException {
        return new URL(url).getHost();
    }
}
