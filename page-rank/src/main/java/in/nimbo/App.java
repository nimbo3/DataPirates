package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Edge;
import in.nimbo.model.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.spark.rdd.EsSpark;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.graphframes.GraphFrame;
import org.apache.commons.codec.digest.DigestUtils;


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

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);
        hbaseConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "500");

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.executor.cores", String.valueOf(sparkExecutorCores))
                .set("spark.executor.memory", sparkExecutorMemory)
                .set("spark.cores.max", String.valueOf(sparkExecutorCores * sparkExecutorNumber))
                .set("es.nodes", elasticNodesIp)
                .set("es.mapping.id", "id")
//                .set("es.mapping.page-rank", "pageRank")
                .set("es.write.operation", "upsert");


        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .toJavaRDD().map(tuple -> tuple._2);

        JavaRDD<Cell> cellRDD = hbaseRDD.flatMap(result -> result.listCells().iterator());

        JavaRDD<Vertex> vertexRDD = cellRDD.map(cell -> new Vertex(Bytes.toString(CellUtil.cloneRow(cell))));
        JavaRDD<Edge> edgeRDD = cellRDD.map(cell -> {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + " <-> " + Bytes.toString(CellUtil.cloneQualifier(cell)));
            return new Edge(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneRow(cell)));
        });

        Dataset<Row> vertexDF = sparkSession.createDataFrame(vertexRDD, Vertex.class);
        Dataset<Row> edgeDF = sparkSession.createDataFrame(edgeRDD, Edge.class);

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        GraphFrame pageRankResult = graphFrame.pageRank().maxIter(10).run();
        pageRankResult.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<Row> map = pageRankResult.vertices().toJavaRDD().map(row -> {
            System.out.println("DF:String " + row.getString(0));
            System.out.println("DF:DoubleRank " + row.getDouble(1));
            return row;
        });

        JavaRDD<UpdateObject> elasticRDD = pageRankResult.vertices().toJavaRDD().map(row -> {
            System.out.println("link: " + row.getString(0) + ", rank: " + row.getDouble(1));
            System.out.println("----------------------------------------------------------------------------------------");
            return new UpdateObject(DigestUtils.sha256Hex(row.getString(0)), row.getString(0), row.getDouble(1));
        });
        EsSpark.saveToEs(elasticRDD.rdd(), elasticIndexName);

        sparkSession.close();
    }
}
