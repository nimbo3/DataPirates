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
    private WebGraphResult webGraphResult;
    private Logger logger = LoggerFactory.getLogger(WebGraphDomains.class);

    private static WebGraphDomains instance = new WebGraphDomains();

    public static WebGraphDomains getInstance() {
        return instance;
    }

    private WebGraphDomains() {
    }

    public void load(Hbase hbase) {
        try {
            List<String> topDomains = getTopDomainsList();

            Result[] results = hbase.get(topDomains);

            Set<Edge> edges = new LinkedHashSet<>();
            Set<Vertex> vertices = new LinkedHashSet<>();
            for (Result result : results) {
                extractHbaseResultToGraph(edges, vertices, result);
            }
            Set<Edge> limitedEdges = new LinkedHashSet<>();
            Set<Vertex> limitedVertices = new LinkedHashSet<>();
            edges.forEach(edge -> {
                if (topDomains.contains(edge.getSrc()) && topDomains.contains(edge.getDst()))
                    limitedEdges.add(edge);
            });

            vertices.forEach(vertex -> {
                if (topDomains.contains(vertex.getId()))
                    limitedVertices.add(vertex);
            });

            webGraphResult = new WebGraphResult(new ArrayList<>(limitedVertices), new ArrayList<>(limitedEdges));
        } catch (IOException e) {
            logger.error("Error in fetching top domains", e);
        } catch (HbaseException e) {
            logger.error("Error in getting domain rows from hbase", e);
        }

    }

    public static void extractHbaseResultToGraph(Set<Edge> edges, Set<Vertex> vertices, Result result) {
        String row = Bytes.toString(result.getRow());
        if (row != null && result.listCells() != null) {
            vertices.add(new Vertex(row, "#17a2b8"));
            result.listCells().forEach(cell -> {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                if (value > 5) {
                    value = 5;
                }
                vertices.add(new Vertex(qualifier));
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

    private List<String> getTopDomainsList() throws IOException {
        String url = "https://www.alexa.com/topsites";
        Document document = Jsoup.connect(url)
                .followRedirects(true)
                .timeout(20000)
                .get();

        List<String> topDomains = new ArrayList<>();
        for (Element urlContainer : document.getElementsByClass("DescriptionCell")) {
            topDomains.add(urlContainer.select("a").get(0).text().toLowerCase());
        }
        return topDomains;
    }



    public WebGraphResult getWebGraphResult() {
        return webGraphResult;
    }
}
