package in.nimbo;

import com.typesafe.config.Config;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HbaseCacheDao {
    private static final Logger logger = LoggerFactory.getLogger(HbaseCacheDao.class);
    private final String CACHE_TABLE_NAME;
    private Connection connection;
    private String FLAG_FAMILY;

    public HbaseCacheDao(Connection connection, Config config) {
        this.connection = connection;
        CACHE_TABLE_NAME = config.getString("hbase.cache.table.name");
        FLAG_FAMILY = config.getString("hbase.cache.table.column.family");
    }

    public List<String> scan(){
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FLAG_FAMILY));
        scan.setFilter(new FirstKeyOnlyFilter());
        List<String> list = new LinkedList<>();
        try(Table table = connection.getTable(TableName.valueOf(CACHE_TABLE_NAME));
            ResultScanner scanner = table.getScanner(scan)) {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                list.add(Bytes.toString(result.getRow()));
            }
        } catch (IOException e) {
            logger.error("can't connect to table with name: " + CACHE_TABLE_NAME);
        }
        return list;
    }
}
