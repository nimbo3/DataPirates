package in.nimbo;

import in.nimbo.model.Edge;
import in.nimbo.model.Hbase;
import in.nimbo.model.Vertex;
import in.nimbo.model.WebGraphResult;
import in.nimbo.model.exceptions.HbaseException;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class WebGraphDomains {
    private static WebGraphResult webGraphResult;
    private static Logger logger = LoggerFactory.getLogger(WebGraphDomains.class);
    private static Hbase hbase;

    public static void load(Hbase _hbase) {
        hbase = _hbase;
        try {
            List<String> topDomains = getTopDomainsList();
            System.out.println(topDomains.size());

//            List<Result> results = new ArrayList<>();
//            int counter = 0;
//            for (String domain : topDomains) {
//                if (counter > 20)
//                    break;
//                results.add(hbase.get(domain));
//                counter++;
//            }
            Result[] results = hbase.get(topDomains);
//            System.out.println("results.size() = " + results.size());

            Set<Edge> edges = new LinkedHashSet<>();
            Set<Vertex> verteces = new LinkedHashSet<>();
            for (Result result : results) {
                extractHbaseResultToGraph(edges, verteces, result);
            }
            System.out.println("verteces = " + verteces.size());
            System.out.println("edges = " + edges);

            Set<Edge> reorderedEdges = new LinkedHashSet<>();
            Set<Vertex> reorderedVerteces = new LinkedHashSet<>();
            verteces.forEach(vertex -> {
                if (topDomains.contains(vertex.getId()))
                    reorderedVerteces.add(vertex);
            });
            edges.forEach(edge -> {
                if (topDomains.contains(edge.getSrc()) && topDomains.contains(edge.getDst()))
                    reorderedEdges.add(edge);
            });

            System.out.println("reorderedEdges.size() = " + reorderedEdges.size());
            System.out.println("reorderedVerteces.size() = " + reorderedVerteces.size());

            webGraphResult = new WebGraphResult(new ArrayList<>(reorderedVerteces), new ArrayList<>(reorderedEdges));
        } catch (IOException e) {
            logger.error("Error in fetching top domains", e);
        } catch (HbaseException e) {
            logger.error("Error in getting domain rows from hbase", e);
        }

    }

    public static void extractHbaseResultToGraph(Set<Edge> edges, Set<Vertex> verteces, Result result) {
        String row = Bytes.toString(result.getRow());
        System.out.println(row);
        if (row != null) {
            if (result.listCells() != null)
                System.out.println(result.listCells().size());
            verteces.add(new Vertex(row, "#17a2b8"));
            result.listCells().forEach(cell -> {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                if (value > 5) {
                    value = 5;
                }
                verteces.add(new Vertex(qualifier));
                Edge edge;
                if (!row.equals(qualifier)) {
                    if (family.equals("i")) {
                        edge = new Edge(qualifier, row, value);
                    } else {
                        edge = new Edge(row, qualifier, value);
                    }
                    edges.add(edge);
                }
            });
        }
    }

    private static List<String> getTopDomainsList() throws IOException {
        Document document = Jsoup.connect("https://www.alexa.com/topsites")
                .followRedirects(true)
                .timeout(30000)
                .get();

        List<String> topDomains = new ArrayList<>();
        for (Element urlContainer : document.getElementsByClass("DescriptionCell")) {
            topDomains.add(urlContainer.select("a").get(0).text().toLowerCase());
        }
        return topDomains;
    }

    public static WebGraphResult getWebGraphResult() {
        return webGraphResult;
    }
}
