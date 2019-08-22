package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Edge;
import in.nimbo.model.Vertex;
import org.apache.commons.lang3.ArrayUtils;
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
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


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
        hbaseReadConfiguration.set(TableInputFormat.SCAN_CACHEDROWS, "2");

        Configuration hbaseWriteConfiguration = HBaseConfiguration.create();
        hbaseReadConfiguration.addResource(hbaseXmlHadoop);
        hbaseReadConfiguration.addResource(hbaseXmlHbase);
        hbaseWriteConfiguration.set(TableInputFormat.INPUT_TABLE, hbaseWriteTableName);

        Job newAPIJobConfiguration = Job.getInstance(hbaseWriteConfiguration);
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hbaseWriteTableName);
        newAPIJobConfiguration.setOutputFormatClass(TableOutputFormat.class);

        SparkConf sparkConf = new SparkConf()
                .setAppName(sparkAppName)
                .set("spark.cores.max", "3")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.executor.cores", sparkExecutorCores)
                .set("spark.executor.memory", sparkExecutorMemory);
        sparkConf.registerKryoClasses(ArrayUtils.toArray(Edge.class, Vertex.class));


        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        LongAccumulator verticesSize = sparkSession.sparkContext().longAccumulator();
        LongAccumulator domainToDomainPairSize = sparkSession.sparkContext().longAccumulator();
        LongAccumulator domainToDomainPairWeightSize = sparkSession.sparkContext().longAccumulator();

        JavaRDD<Result> hbaseRDD = sparkSession.sparkContext().newAPIHadoopRDD(hbaseReadConfiguration
                , TableInputFormat.class, ImmutableBytesWritable.class, Result.class).toJavaRDD().map(tuple -> tuple._2);

        JavaRDD<Cell> hbaseCellsJavaRDD = hbaseRDD.flatMap(result -> result.listCells().iterator());

        hbaseCellsJavaRDD.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<Vertex> vertexJavaRDD = hbaseRDD.flatMap(result -> {
            try {
                String domain = getDomain(DEFAULT_PROTOCOL + Bytes.toString(result.getRow()));
                verticesSize.add(1);
                System.out.println(String.format("Vertix= %s ", domain));
                return Collections.singleton(new Vertex(domain)).iterator();
            } catch (MalformedURLException e) {
                System.out.println("Exception In Mapping Vertix");
                return Collections.emptyIterator();
            }
        });

        vertexJavaRDD.foreach(vertex -> System.out.println(vertex.getId()));

        JavaRDD<Edge> edgeJavaRDD = hbaseCellsJavaRDD.flatMap(cell -> {
            String source = Bytes.toString(CellUtil.cloneRow(cell));
            String destination = Bytes.toString(CellUtil.cloneQualifier(cell));
            try {
                String sourceDomain = getDomain(DEFAULT_PROTOCOL + source);
                String destinationDomain = getDomain(DEFAULT_PROTOCOL + destination);
                domainToDomainPairSize.add(1);
                System.out.println(String.format("Edge= %s -> %s", sourceDomain, destinationDomain));
                return Collections.singleton(new Edge(sourceDomain, destinationDomain)).iterator();
            } catch (MalformedURLException e) {
                System.out.println("Exception In Mapping Edge");
                return Collections.emptyIterator();
            }
        });

        edgeJavaRDD.foreach(edge -> System.out.println(edge.getSrc() + " : " + edge.getDst()));

        Dataset<Row> vertexDF = sparkSession.createDataFrame(vertexJavaRDD, Vertex.class);
        Dataset<Row> edgeDF = sparkSession.createDataFrame(edgeJavaRDD, Edge.class);

        GraphFrame graphFrame = new GraphFrame(vertexDF, edgeDF);

        graphFrame.persist(StorageLevel.MEMORY_ONLY());

//        JavaPairRDD<String, String> domainToDomainPair =
        graphFrame.triplets().toJavaRDD().foreach(row -> {
            String srcDomain = row.getStruct(1).getString(1);
            String dstDomain = row.getStruct(1).getString(0);
            int num = row.getStruct(1).getInt(2);
            System.out.println("%%% " + srcDomain + " : " + dstDomain + " : " + num);
//            return new Tuple2<>(srcDomain, dstDomain);
        });

//        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairRDD = graphFrame.triplets().toJavaRDD().mapToPair(row -> {
//            Edge edge = (Edge) row.get(1);
//            Tuple2<String, String> domainPair = new Tuple2<>(edge.getSrc(), edge.getDst());
//            System.out.println(String.format("pair= %s -> %s", edge.getSrc(), edge.getDst()));
//            return new Tuple2<>(domainPair, edge.getWeight());
//        });
//
//
//        JavaPairRDD<Tuple2<String, String>, Integer> domainToDomainPairWeightRDD = domainToDomainPairRDD
//                .reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
//
//        domainToDomainPairWeightRDD.foreach((VoidFunction<Tuple2<Tuple2<String, String>, Integer>>) tuple2IntegerTuple2 -> {
//            domainToDomainPairWeightSize.add(1);
//            System.out.println(String.format("reducedPair= %s -> %s : %d", tuple2IntegerTuple2._1._1, tuple2IntegerTuple2._1._2, tuple2IntegerTuple2._2));
//        });
//
//        System.out.println("Domain To Domain Pair Size :  " + domainToDomainPairSize.sum());
//        System.out.println("Domain To Domain Pair Weighted Size :  " + domainToDomainPairWeightSize.sum());
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
