package in.nimbo;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class App {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseTableName = config.getString("hbase.table.name");
        String hbaseColumnFamily = config.getString("hbase.column.family");

        Configuration hbaseConfiguration = HBaseConfiguration.create();


        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);
        hbaseConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "500");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName(sparkAppName);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Result> hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .values();
        JavaRDD<Cell> cellRDD = hbaseRDD.flatMap(result -> {
            result.listCells().forEach(t -> System.out.println(t));
            return result.listCells().iterator();
        });
        JavaPairRDD<byte[], Integer> linkToOne = cellRDD.mapToPair(cell -> new Tuple2<>(CellUtil.cloneQualifier(cell), 1));
        JavaPairRDD<byte[], Integer> linkToCount = linkToOne.reduceByKey(Integer::sum);

        linkToCount.foreach(t -> System.out.println(new String(t._1) + ":" + t._2));
        sparkContext.close();

    }
}