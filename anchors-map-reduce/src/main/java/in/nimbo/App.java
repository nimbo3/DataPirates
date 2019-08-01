package in.nimbo;

import com.google.protobuf.ServiceException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NavigableMap;


public class App {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseTableName = config.getString("hbase.table.name");
        String hbaseColumnFamily = config.getString("hbase.column.family");

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        try {
            HBaseAdmin.checkHBaseAvailable(hbaseConfiguration);
            System.err.println("-----------------------------------------------------------------------------------------------------------------------------------");
            System.err.println("Hbase available!");
            System.err.println("-----------------------------------------------------------------------------------------------------------------------------------");
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(sparkAppName);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Result> hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .values();

        JavaPairRDD<String, ArrayList<String>> mapResult = hbaseRDD.flatMapToPair((PairFlatMapFunction<Result, String, ArrayList<String>>) row -> {
            ArrayList<Tuple2<String, ArrayList<String>>> result = new ArrayList<>();
            NavigableMap<byte[], byte[]> familyMap = row.getFamilyMap(Bytes.toBytes(hbaseColumnFamily));
            familyMap.forEach((qualifier, value) -> {
                String link = Bytes.toString(qualifier);
                String text = Bytes.toString(value);
                System.out.println("link: " + link);
                System.out.println("text: " + text);
                result.add(new Tuple2<>(link, new ArrayList<>(Collections.singletonList(text))));
            });
            return result.iterator();
        });

    }
}