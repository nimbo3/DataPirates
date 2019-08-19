package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Edge;
import in.nimbo.model.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.graphframes.GraphFrame;


public class App {

    public static final String DEFAULT_PROTOCOL = "http://";
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws IOException {
        Config config = ConfigFactory.load("config");
        String sparkAppName = config.getString("spark.app.name");
        String hbaseXmlHadoop = config.getString("hbase.xml.url.in.hadoop");
        String hbaseXmlHbase = config.getString("hbase.xml.url.in.hbase");
        String hbaseReadTableName = config.getString("hbase.read.table.name");
        String hbaseReadColumnFamily = config.getString("hbase.read.column.family");
        String hbaseWriteTableName = config.getString("hbase.write.table.name");
        String hbaseWriteColumnFamilyInput = config.getString("hbase.write.column.family.input.domains");
        String hbaseWriteColumnFamilyOutput = config.getString("hbase.write.column.family.output.domains");
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

        Job newAPIJobConfiguration = Job.getInstance(hbaseWriteConfiguration);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseWriteTableName);
        newAPIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.executor.cores", sparkExecutorCores)
                .set("spark.executor.memory", sparkExecutorMemory);

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        LongAccumulator domainToDomainPairSize = sparkSession.sparkContext().longAccumulator();
        LongAccumulator domainToDomainPairWeightedSize = sparkSession.sparkContext().longAccumulator();

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseReadConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class).toJavaRDD().map(tuple -> tuple._2);

        JavaRDD<Cell> hbaseCellsJavaRDD = hbaseRDD.flatMap(result -> result.listCells().iterator());

        JavaRDD<Vertex> vertexJavaRDD = hbaseCellsJavaRDD.map(cell -> new Vertex(Bytes.toString(CellUtil.cloneQualifier(cell))));
        JavaRDD<Edge> edgeJavaRDD = hbaseCellsJavaRDD.map(cell -> new Edge(Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneQualifier(cell))));

        Dataset<Row> vertexDF = sparkSession.createDataFrame(vertexJavaRDD, Vertex.class);
        Dataset<Row> edgeDF = sparkSession.createDataFrame(edgeJavaRDD, Edge.class);

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);
        graphFrame.triplets().toJavaRDD().foreach(row -> {
            Vertex srcVertex = (Vertex) row.get(0);
            Edge edge = (Edge) row.get(1);
            Vertex dstVertex = (Vertex) row.get(2);
            System.out.println("srcVertex " + srcVertex.getId());
            System.out.println("edge " + edge.getSrc() + ":" + edge.getDst());
            System.out.println("dstVertex " + dstVertex.getId());
        });

//        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairRDD = hbaseCellsJavaRDD.flatMapToPair(cell -> {
//            String source = new String(CellUtil.cloneRow(cell));
//            String destination = new String(CellUtil.cloneQualifier(cell));
//            try {
//                String sourceDomain = getDomain(DEFAULT_PROTOCOL + source);
//                String destinationDomain = getDomain(DEFAULT_PROTOCOL + destination);
//                Tuple2<String, String> domainPair = new Tuple2<>(sourceDomain, destinationDomain);
//                domainToDomainPairSize.add(1);
//                return Collections.singleton(new Tuple2<>(domainPair, 1)).iterator();
//            } catch (MalformedURLException e) {
//                return Collections.emptyIterator();
//            }
//        });
//
//        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairWeightRDD = domainToDomainPairRDD
//                .reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
//
//        domainToDomainPairWeightRDD.foreach((VoidFunction<Tuple2<Tuple2<String, String>, Integer>>) tuple2IntegerTuple2 -> {
//            domainToDomainPairWeightedSize.add(1);
//            System.out.println(String.format("%s -> %s : %d", tuple2IntegerTuple2._1._1, tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
//        });
//
//        System.err.println("Domain To Domain Pair Size :  " + domainToDomainPairSize.sum());
//        System.err.println("Domain To Domain Pair Weighted Size :  " + domainToDomainPairWeightedSize.sum());
//
//        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = domainToDomainPairWeightRDD
//                .flatMapToPair(t -> {
//                    byte[] sourceDomainBytes = Bytes.toBytes(t._1._1);
//                    byte[] destinationDomainBytes = Bytes.toBytes(t._1._2);
//                    byte[] domainToDomainRefrences = Bytes.toBytes(t._2);
//
//                    Set<Tuple2<ImmutableBytesWritable, Put>> hbasePut = new HashSet<>();
//
//                    Put outputDomainPut = new Put(sourceDomainBytes);
//                    outputDomainPut.addColumn(Bytes.toBytes(hbaseWriteColumnFamilyOutput),
//                            destinationDomainBytes,
//                            domainToDomainRefrences);
//
//                    Put inputDomainPut = new Put(destinationDomainBytes);
//                    inputDomainPut.addColumn(Bytes.toBytes(hbaseWriteColumnFamilyInput),
//                            sourceDomainBytes,
//                            domainToDomainRefrences);
//
//                    hbasePut.add(new Tuple2<>(new ImmutableBytesWritable(), inputDomainPut));
//                    hbasePut.add(new Tuple2<>(new ImmutableBytesWritable(), outputDomainPut));
//                    return hbasePut.iterator();
//                });
//
//        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());

        sparkSession.stop();
    }

    public static String getDomain(String url) throws MalformedURLException {
        return new URL(url).getHost();
    }
}
