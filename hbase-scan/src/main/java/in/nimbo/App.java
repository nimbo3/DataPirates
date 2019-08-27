package in.nimbo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException {
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(config);

        // Instantiating the Scan class
        Scan scan = new Scan();
        scan.setMaxResultSize(2000000);

        // Scanning the required families
        scan.addFamily(Bytes.toBytes("l"));

        // Getting the scan result
        Table table = connection.getTable(TableName.valueOf("s"));
        ResultScanner scanner = table.getScanner(scan);
        table.close();

        Table putTable = connection.getTable(TableName.valueOf("p"));
        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            Put put = new Put(result.getRow());
            for (Cell cell : result.listCells()) {
                put.addColumn("l".getBytes(), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
            }
            System.out.println("Row : " + new String(result.getRow()) + " -> " + result.listCells().size());
            putTable.put(put);
        }
        putTable.close();
        table.close();
        //closing the scanner
        scanner.close();
    }
}
