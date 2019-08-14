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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class PageRankApp {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHbase = "~/Desktop/tmp/hbase/conf/hbase-site.xml";
        String hbaseTableName = "t";
        String hbaseColumnFamily = "l";

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .setMaster("local[*]")
                ;
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Result> hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .values();
        JavaRDD<Cell> cellJavaRDD = hbaseRDD.flatMap(result -> {
            System.out.println(result.listCells().size());
            return result.listCells().iterator();
        });

    }
}
