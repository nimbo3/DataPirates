package in.nimbo;

import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.util.CellUtility;
import in.nimbo.util.HashGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;


public class App {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseTableName = config.getString("hbase.table.name");
        String hbaseColumnFamily = config.getString("hbase.column.family");
        int sparkExecutorCores = config.getInt("spark.executor.cores");
        String sparkExecutorMemory = config.getString("spark.executor.memory");
        int sparkExecutorNumber = config.getInt("spark.executor.number");
        String elasticNodesIp = config.getString("es.nodes.ip");
        String elasticIndexName = config.getString("es.index.name");
        String elasticAutoCreateIndex = config.getString("es.index.auto.create");
        int pageRankIterNum = config.getInt("page.rank.iter.num");

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.executor.cores", String.valueOf(sparkExecutorCores))
                .set("spark.executor.memory", sparkExecutorMemory)
                .set("spark.cores.max", String.valueOf(sparkExecutorCores * sparkExecutorNumber))
                .set("es.nodes", elasticNodesIp)
                .set("es.mapping.id", "id")
                .set("es.index.auto.create", elasticAutoCreateIndex)
                .set("es.write.operation", "upsert");

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        Set hashedRowKeys = null;
        try {
            hashedRowKeys = createRowKeysHashSet(hbaseTableName, hbaseColumnFamily);
            System.out.println("hash set created successfully!!");
        } catch (IOException e) {
            // do sth
            System.out.println("couldn't connect to hbase table!!!");
            System.exit(1);
        }

        Broadcast<Set> broadcastVar = javaSparkContext.broadcast(hashedRowKeys);

        Set broadcastHashSet = broadcastVar.getValue();

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .toJavaRDD().map(tuple -> tuple._2);


        JavaPairRDD<String, Iterable<String>> links = hbaseRDD.mapToPair(result -> new Tuple2<>(new String(result.getRow()), getIterableFromIterator(result.listCells().stream().filter(cell -> broadcastHashSet.contains(CellUtility.getCellQualifier(cell))).map(cell -> CellUtility.getCellQualifier(cell)).iterator()))).cache();
        broadcastVar.destroy();

        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        for (int current = 0; current < pageRankIterNum; current++) {
            ranks.foreach(t -> {
                System.out.println(String.format("link: %s -> rank: %s", t._1, t._2));
            });
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2 / urlCount));
                        }
                        return results.iterator();
                    });
            ranks = contribs.reduceByKey(Double::sum).mapValues(sum -> 0.15 + sum * 0.85);
        }

        ranks.foreach(t -> {
            System.out.println(String.format("link: %s -> rank: %s", t._1, t._2));
        });
//        JavaRDD<UpdateObject> elasticRDD = ranks.map(tuple -> new UpdateObject(HashGenerator.md5HashString(tuple._1), tuple._1, tuple._2));
//        JavaEsSpark.saveToEs(elasticRDD, elasticIndexName);

        sparkSession.close();
    }

    private static <T> Iterable<T> getIterableFromIterator(Iterator<T> iterator) {
        return () -> iterator;
    }

    private static Set<String> createRowKeysHashSet(String tableName, String columnFamily) throws IOException {
        System.out.println("initializing configuration for scanning hbase ...");
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        scan.setFilter(new FirstKeyOnlyFilter());
        Table table = connection.getTable(TableName.valueOf(tableName));

        ResultScanner scanner = table.getScanner(scan);
        table.close();
        System.out.println("Scanning ...");

        Set<String> hashedRows = new HashSet<>();
        int counter = 0;
        for (Result result : scanner) {
            counter++;
            hashedRows.add(HashGenerator.md5HashString(result.getRow()));
            if (counter % 1000 == 0)
                System.out.println("counter: " + counter);
        }
        System.out.println("hash set is ready now!");

        return hashedRows;
    }
}
