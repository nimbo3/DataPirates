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
import org.apache.spark.util.LongAccumulator;
//import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.List;


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
        String elasticNodesIp = config.getString("es.nodes.ip");
        String elasticIndexName = config.getString("es.index.name");
        String elasticTableName = config.getString("es.table.name");


        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);
        hbaseConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "500");

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.executor.cores", sparkExecutorCores)
                .set("spark.executor.memory", sparkExecutorMemory)
                .setMaster("local[*]")
//                .set("es.nodes", elasticNodesIp)
//                .set("es.mapping.id", "id")
                // todo
//                .set("es.write.operation", "update")
                ;
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        LongAccumulator rowCount = sparkContext.sc().longAccumulator();
        LongAccumulator cellCount = sparkContext.sc().longAccumulator();

        JavaRDD<Result> hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .values();
        JavaRDD<Cell> cellRDD = hbaseRDD.flatMap(result -> {
            List<Cell> cells = result.listCells();
            rowCount.add(1);
            cellCount.add(cells.size());
            return cells.iterator();
        });
        JavaPairRDD<byte[], Integer> linkToOne = cellRDD.mapToPair(cell -> new Tuple2<>(CellUtil.cloneQualifier(cell), 1));
        JavaPairRDD<byte[], Integer> linkToCount = linkToOne.reduceByKey(Integer::sum);

        System.err.println("row count: "+rowCount.value());
        System.err.println("cell count: "+cellCount.value());
        JavaRDD<UpdateObject> updateObjectJavaRDD = linkToCount.map(t -> new UpdateObject(new String(t._1), t._2));
        linkToCount.foreach(t -> {
            if (t._2 > 1)
                System.out.println(new String(t._1) + ":" + t._2);
        });
//        JavaEsSpark.saveToEs(updateObjectJavaRDD, elasticIndexName + "/" + elasticTableName);
        sparkContext.close();

    }
}