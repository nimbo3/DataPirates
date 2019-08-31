package in.nimbo;

import com.typesafe.config.Config;
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

public class TopDomains {
    private static WebGraphResult webGraphResult;
    private static Logger logger = LoggerFactory.getLogger(TopDomains.class);
    private static Hbase hbase;

    public static void load(Hbase _hbase) {
        hbase = _hbase;
        try {
            List<String> topDomains = getTopDomainsList();

            List<Result> results = new ArrayList<>();
            for (String domain : topDomains) {
                results.add(hbase.get(domain));
            }

            Set<Edge> edges = new LinkedHashSet<>();
            Set<Vertex> verteces = new LinkedHashSet<>();
            for (Result result : results) {
                extractHbaseResultToGraph(edges, verteces, result);
            }

            webGraphResult = new WebGraphResult(new ArrayList<>(verteces), new ArrayList<>(edges));
        } catch (IOException e) {
            logger.error("Error in fetching top domains", e);
        } catch (HbaseException e) {
            logger.error("Error in getting domain rows from hbase", e);
        }

    }

    public static void extractHbaseResultToGraph(Set<Edge> edges, Set<Vertex> verteces, Result result) {
        verteces.add(new Vertex(Bytes.toString(result.getRow()), "#17a2b8"));
        result.listCells().forEach(cell -> {
            String row = Bytes.toString(CellUtil.cloneRow(cell));
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

    private static List<String> getTopDomainsList() throws IOException {
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
