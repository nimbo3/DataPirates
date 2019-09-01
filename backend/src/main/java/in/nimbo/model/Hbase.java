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
import java.util.ArrayList;
import java.util.List;

public class Hbase {
    private static final Logger logger = LoggerFactory.getLogger(Hbase.class);
    private final String TABLE_NAME;
    private Connection connection;

    public Hbase(Connection connection, Config config) {
        TABLE_NAME = config.getString("hbase.table.name");
        this.connection = connection;
    }

    public Result get(String domain) throws HbaseException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(domain));
            return table.get(get);
        } catch (IOException e) {
            throw new HbaseException("Can't get from Hbase", e);
        }
    }

    public Result[] get(List<String> domains) throws HbaseException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            List<Get> getList = new ArrayList<>();
            for (String domain : domains) {
                getList.add(new Get(Bytes.toBytes(domain)));
            }
            return table.get(getList);
        } catch (IOException e) {
            throw new HbaseException("Can't get from Hbase", e);
        }
    }


    public void closeConnection() throws IOException {
        connection.close();
    }
}

