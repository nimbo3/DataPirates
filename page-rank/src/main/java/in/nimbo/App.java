package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.UpdateObject;
import in.nimbo.util.CellUtility;
import in.nimbo.util.HashGenerator;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
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

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);
//        hbaseConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "500");

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

        JavaRDD<Cell> cellRDD = hbaseRDD.flatMap(result -> result.listCells().iterator());


        cellRDD.persist(StorageLevel.MEMORY_AND_DISK());

        Set hashedRowKeys = new HashSet();
        cellRDD.foreach(cell -> {
            String rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            String hashedRowKey = new String(DigestUtils.md5(rowKey), StandardCharsets.UTF_8);
            hashedRowKeys.add(hashedRowKey);
        });

        Broadcast<Set> broadcastVar = javaSparkContext.broadcast(hashedRowKeys);

        Set broadcastHashSet = broadcastVar.getValue();
        JavaRDD<Cell> cellEdgeRDD = cellRDD.filter(cell -> broadcastHashSet.contains(HashGenerator.md5HashString(CellUtility.getCellQualifier(cell))));
        broadcastVar.destroy();


//        sparkSession.sparkContext().broadcast(hashedRowKeys, classTag(HashSet.class));
//        cellRDD.foreachPartition(cellIterator -> {
//            while (cellIterator.hasNext()){
//                Cell cell = cellIterator.next();
//            }
//        });
//
        cellRDD.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<UpdateObject> elasticRDD = null;

        JavaEsSpark.saveToEs(elasticRDD, elasticIndexName);

        sparkSession.close();
    }
}
