package in.nimbo.model;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.model.exceptions.HbaseException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Hbase {
    private static final Logger logger = LoggerFactory.getLogger(Hbase.class);
    private final String TABLE_NAME;
    private Timer getTimer = SharedMetricRegistries.getDefault().timer("hbase-get");
    private Connection connection;

    private boolean closed = false;


    public Hbase(Connection connection, Config config) {
        TABLE_NAME = config.getString("hbase.table.name");
        this.connection = connection;
    }


    public Result get(String domain) throws HbaseException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Timer.Context time = getTimer.time()) {
            Get get = new Get(Bytes.toBytes(domain));
            return table.get(get);
        } catch (IOException e) {
            logger.error("Can't get from hbase", e);
            throw new HbaseException("can't get from Hbase", e);
        }
    }

    public void closeConnection() throws IOException {
        connection.close();
    }
}

