package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Edge;
import in.nimbo.model.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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
import org.graphframes.GraphFrame;

public class PageRankApp {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseTableName = config.getString("hbase.table.name");
        String hbaseColumnFamily = config.getString("hbase.column.family");
        String sparkExecutorCores = config.getString("spark.executor.cores");
        String sparkExecutorMemory = config.getString("spark.executor.memory");

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
                ;

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate()
                ;

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class)
                .toJavaRDD().map(tuple -> tuple._2);

        JavaRDD<Cell> cellJavaRDD = hbaseRDD.flatMap(result -> {
            System.out.println(result.listCells().size());
            return result.listCells().iterator();
        });

        JavaRDD<Vertex> vertexJavaRDD = cellJavaRDD.map(cell -> new Vertex(Bytes.toString(cell.getQualifierArray())));
        JavaRDD<Edge> edgeJavaRDD = cellJavaRDD.map(cell -> new Edge(Bytes.toString(cell.getRowArray()), Bytes.toString(cell.getQualifierArray())));

        Dataset<Row> vertexDF = sparkSession.createDataFrame(vertexJavaRDD, Vertex.class);
        Dataset<Row> edgeDF = sparkSession.createDataFrame(edgeJavaRDD, Edge.class);

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        GraphFrame pageRankResult = graphFrame.pageRank().maxIter(10).run();
        pageRankResult.persist(StorageLevel.MEMORY_ONLY());

        pageRankResult.vertices().toJavaRDD().map(row -> {
            System.out.println(row.getString(0));
            System.out.println(row.getDouble(1));
            return row;
        });

        sparkSession.close();
    }
}
