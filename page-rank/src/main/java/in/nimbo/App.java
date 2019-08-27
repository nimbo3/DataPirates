package in.nimbo;

import com.google.common.collect.Iterables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.UpdateObject;
import in.nimbo.util.CellUtility;
import in.nimbo.util.HashGenerator;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


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

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .toJavaRDD().map(tuple -> tuple._2);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaPairRDD<String, String> edges = hbaseRDD.flatMap(result -> result.listCells().iterator()).mapToPair(cell -> new Tuple2<>(CellUtility.getCellRowString(cell), CellUtility.getCellQualifier(cell)));

        edges.persist(StorageLevel.MEMORY_AND_DISK());

        Set hashedRowKeys = new HashSet();
        edges.foreach(edge -> {
            String hashedRowKey = new String(DigestUtils.md5(edge._1), StandardCharsets.UTF_8);
            hashedRowKeys.add(hashedRowKey);
        });

        Broadcast<Set> broadcastVar = javaSparkContext.broadcast(hashedRowKeys);

        Set broadcastHashSet = broadcastVar.getValue();
        JavaPairRDD<String, String> validEdges = edges.filter(edge -> broadcastHashSet.contains(HashGenerator.md5HashString(edge._2)));
        broadcastVar.destroy();

        JavaPairRDD<String, Iterable<String>> links = validEdges.groupByKey().cache();
        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        for (int current = 0; current < pageRankIterNum; current++) {
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int urlCount = Iterables.size(s._1());
                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n : s._1) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });
            ranks = contribs.reduceByKey(Double::sum).mapValues(sum -> 0.15 + sum * 0.85);
        }

        JavaRDD<UpdateObject> elasticRDD = ranks.map(tuple -> new UpdateObject(HashGenerator.md5HashString(tuple._1), tuple._1, tuple._2));
        JavaEsSpark.saveToEs(elasticRDD, elasticIndexName);

        sparkSession.close();
    }
}
