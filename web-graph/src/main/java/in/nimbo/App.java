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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;


public class App {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseReadTableName = config.getString("hbase.read.table.name");
        String hbaseReadColumnFamily = config.getString("hbase.read.column.family");
        String hbaseWriteTableName = config.getString("hbase.write.table.name");
        String hbaseWriteColumnFamily = config.getString("hbase.write.column.family");
        String hbaseWriteQualifier = config.getString("hbase.write.qualifier");
        String sparkExecutorCores = config.getString("spark.executor.cores");
        String sparkExecutorMemory = config.getString("spark.executor.memory");

        Configuration hbaseReadConfiguration = HBaseConfiguration.create();
        hbaseReadConfiguration.addResource(hbaseXmlHadoop);
        hbaseReadConfiguration.addResource(hbaseXmlHbase);
        hbaseReadConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseReadTableName);
        hbaseReadConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseReadColumnFamily);
        hbaseReadConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "500");

        Configuration hbaseWriteConfiguration = HBaseConfiguration.create();
        hbaseReadConfiguration.addResource(hbaseXmlHadoop);
        hbaseReadConfiguration.addResource(hbaseXmlHbase);
        hbaseWriteConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseWriteTableName);

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.executor.cores", sparkExecutorCores)
                .set("spark.executor.memory", sparkExecutorMemory);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        LongAccumulator domainToDomainPairSize = sparkContext.sc().longAccumulator();

        JavaRDD<Result> hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseReadConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class).values();

        JavaRDD<Cell> hbaseCells = hbaseRDD.flatMap(result -> result.listCells().iterator());

        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairRDD = hbaseCells.flatMapToPair(cell -> {
            String source = new String(CellUtil.cloneRow(cell));
            String destination = new String(CellUtil.cloneValue(cell));
            try {
                String sourceDomain = getDomain("http://" + source);
                String destinationDomain = getDomain("http://" + destination);
                Tuple2<String, String> domainPair = new Tuple2<>(sourceDomain, destinationDomain);
                domainToDomainPairSize.add(1);
                return Collections.singleton(new Tuple2<>(domainPair, 1)).iterator();
            } catch (MalformedURLException e) {
                return Collections.emptyIterator();
            }
        });

        // Todo : CellUtil.cloneRow() Or getValueArray()

        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairWeightRDD = domainToDomainPairRDD
                .reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);

        domainToDomainPairWeightRDD.foreach((VoidFunction<Tuple2<Tuple2<String, String>, Integer>>) tuple2IntegerTuple2 -> {
            System.out.println(String.format("%s -> %s : %d", tuple2IntegerTuple2._1._1, tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
        });

        System.err.println("Domain To Domain Pair Size :  " + domainToDomainPairSize.sum());

//        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = domainToDomainPairWeightRDD
//                .mapToPair((PairFunction<Tuple2<Tuple2<String, String>, Integer>, ImmutableBytesWritable, Put>) t -> {
//            Put put = new Put(Bytes.toBytes(t._1._1 + ":" + t._1._2));
//            put.addColumn(Bytes.toBytes(hbaseWriteColumnFamily),
//                    Bytes.toBytes(hbaseWriteQualifier),
//                    Bytes.toBytes(t._2));
//
//            return new Tuple2<>(new ImmutableBytesWritable(), put);
//        });

//        hbasePuts.saveAsNewAPIHadoopDataset(hbaseWriteConfiguration);

        sparkContext.close();
    }

    public static String getDomain(String url) throws MalformedURLException {
        return new URL(url).getHost();
    }
}
