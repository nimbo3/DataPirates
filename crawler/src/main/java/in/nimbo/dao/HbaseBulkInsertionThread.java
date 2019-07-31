package in.nimbo.dao;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.model.Site;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class HbaseBulkInsertionThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(HbaseBulkInsertionThread.class);
    private final String TABLE_NAME;
    private Config config;
    private Timer hbasebulkInsertMeter;
    private List<Put> puts = new LinkedList<>();
    private LinkedBlockingQueue<Site> blockingQueue;
    private Connection conn;
    private Configuration hbaseConfig;
    private String anchorsFamily;


    HbaseBulkInsertionThread(Connection conn, Configuration hbaseConfig, LinkedBlockingQueue<Site> blockingQueue, Config config) {
        this.hbasebulkInsertMeter = SharedMetricRegistries.getDefault().timer(config.getString("metric.name.hbase.bulk"));
        this.blockingQueue = blockingQueue;
        this.config = config;
        this.conn = conn;
        this.hbaseConfig = hbaseConfig;
        TABLE_NAME = config.getString("hbase.table.name");
        anchorsFamily = config.getString("hbase.table.column.family.anchors");
    }

    private Connection getConnection() throws IOException {
        if (conn == null)
            conn = ConnectionFactory.createConnection(hbaseConfig);
        return conn;
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (puts.size() >= config.getInt("hbase.bulk.size")) {
                    try (Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
                         Timer.Context time = hbasebulkInsertMeter.time()) {
                        table.put(puts);
                    } catch (IOException e) {
                       logger.error("Hbase thread can't bulk insert.", e);
                    }
                    puts.clear();
                }
                Site site = blockingQueue.take();
                Put put = new Put(Bytes.toBytes(site.getReverseLink()));
                for (Map.Entry<String, String> anchorEntry : site.getAnchors().entrySet()) {
                    String link = anchorEntry.getKey();
                    String text = anchorEntry.getValue();
                    put.addColumn(Bytes.toBytes(anchorsFamily),
                            Bytes.toBytes(link), Bytes.toBytes(text));
                }
                puts.add(put);
            }
        } catch (InterruptedException e) {
            logger.warn("hbase-bulk-insertion thread interrupted!");
        }
    }
}
