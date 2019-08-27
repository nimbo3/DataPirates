package in.nimbo.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;

public class CellUtility {
    public static String getCellRowString(Cell cell) {
        return Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    }

    public static String getCellQualifier(Cell cell) {
        return Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    }

    public static String getCellValue(Cell cell) {
        return Bytes.toString(cell.getValueArray(), cell.getQualifierOffset(), cell.getValueLength());
    }
}
