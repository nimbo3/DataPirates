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
        String sparkExecutorCores = config.getString("spark.executor.cores");
        String sparkExecutorMemory = config.getString("spark.executor.memory");

        String elasticNodesIp = config.getString("es.nodes.ip");
        String elasticIndexName = config.getString("es.index.name");
        String elasticAutoCreateIndex = config.getString("es.index.auto.create");

        Configuration hbaseConfiguration = HBaseConfiguration.create();

        hbaseConfiguration.addResource(hbaseXmlHadoop);
        hbaseConfiguration.addResource(hbaseXmlHbase);
        hbaseConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseTableName);
        hbaseConfiguration.set(TableInputFormat.SCAN_COLUMN_FAMILY, hbaseColumnFamily);
        hbaseConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "1000");

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.cores.max", "12")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.executor.cores", sparkExecutorCores)
                .set("spark.executor.memory", sparkExecutorMemory)
//                .set("es.nodes", elasticNodesIp)
//                .set("es.mapping.id", "id")
//                .set("es.index.auto.create", elasticAutoCreateIndex)
//                .set("es.write.operation", "upsert")
                ;
        sparkConf.registerKryoClasses(new Class[]{
                Edge.class,
                Vertex.class,
                UpdateObject.class
        });


        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .toJavaRDD().map(tuple -> tuple._2);

        JavaRDD<Cell> cellRDD = hbaseRDD.flatMap(result -> result.listCells().iterator());

        cellRDD.persist(StorageLevel.MEMORY_AND_DISK());

        JavaRDD<Vertex> vertexRDD = cellRDD.map(cell -> new Vertex(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())));

        JavaRDD<Edge> edgeRDD = cellRDD.map(cell -> {
            String src = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            String dst = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            return new Edge(src, dst);
        });

        Dataset<Row> vertexDF = sparkSession.createDataFrame(vertexRDD, Vertex.class);
        Dataset<Row> edgeDF = sparkSession.createDataFrame(edgeRDD, Edge.class);

        cellRDD.unpersist();
        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
//        graphFrame.cache();
        GraphFrame pageRankResult = graphFrame.pageRank()
                .resetProbability(0.15).maxIter(1).run();

//        JavaRDD<UpdateObject> elasticRDD =
                pageRankResult.vertices().toJavaRDD()
                        .foreach(row -> System.out.println(String.format("%s -> %f", row.getString(0), row.getDouble(1))));
//                .map(row -> new UpdateObject(DigestUtils.sha256Hex(row.getString(0)), row.getDouble(1)));

//        JavaEsSpark.saveToEs(elasticRDD, elasticIndexName);

        sparkSession.close();
    }
}
