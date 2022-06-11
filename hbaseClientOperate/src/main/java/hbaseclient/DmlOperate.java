package hbaseclient;/*
    @author wxg
    @date 2021/6/15-20:04
    */


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class DmlOperate {
    private static final Connection conn;
    static {
        conn = DdlOperate.getConn();
    }

    /**
     * 方法一：向表中插入数据
     * @param tableName：表名
     * @param rowKey:行号
     * @param columnFamily：列族
     * @param columnName：列名
     * @param value：数据值
     * @throws IOException：IO异常
     */
    public static void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        //  1、向表中插入数据
        Table table = conn.getTable(TableName.valueOf(tableName));
        //  2、创建对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //  3、给put对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        //  4、插入数据
        table.put(put);
        //  5、关闭表连接
        table.close();
    }

    /**
     * 方法二：使用get获取数据
     * @param tableName：表名
     * @param rowKey：行号
     * @param columnFamily：列族
     * @param columnName：列名
     * @throws IOException：IO异常
     */
    public static void getData(String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        //  1、获取表对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //  2、创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //  2-1、获取指定列族的数据
        get.addFamily(Bytes.toBytes(columnFamily));
        //  2-2、获取指定列族和列名的数据
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        //  2-3、获取多个版本的数据
        get.readVersions(5);
        //  3、获取数据
        Result result = table.get(get);
        System.out.println(result);
        //  4、打印数据
        for (Cell cell : result.rawCells()) {
            //  5、打印数据
            byte[] cloneFamily = CellUtil.cloneFamily(cell);
            byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
            byte[] cloneValue = CellUtil.cloneValue(cell);
            System.out.println("columnFamily: " + Bytes.toString(cloneFamily) + "  "+
                                "columnQualifier: " + Bytes.toString(cloneQualifier) + "  " +
                                "columnValue: " + Bytes.toString(cloneValue));
            //  6、关闭表数据
            table.close();
        }

    }

    /**
     * 方法三：查看数据
     * @param tableName：表名
     * @throws IOException：IO异常
     */
    public static void scanTable(String tableName) throws IOException {
        //  1、获取表对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //  2、构建Scan对象
//        Scan scan = new Scan().withStartRow(Bytes.toBytes("1")).withStopRow(Bytes.toBytes("500"));
        Scan scan = new Scan().withStartRow(Bytes.toBytes("1"));
        //  3、扫描表
        ResultScanner resultScanner = table.getScanner(scan);
        //  4、解析resultScanner（这里的result：不同rowkey的数组）
        for (Result result : resultScanner) {
            //  5、解析result并打印(这里的cell指得是：相同的rowkey，不同的value)
            for (Cell cell : result.rawCells()) {
                //  6、打印数据
                byte[] cloneRow = CellUtil.cloneRow(cell);
                byte[] cloneFamily = CellUtil.cloneFamily(cell);
                byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
                byte[] cloneValue = CellUtil.cloneValue(cell);
                System.out.println(
                        "columnRow: " + Bytes.toString(cloneRow) + "  " +
                        "columnFamily: " + Bytes.toString(cloneFamily) + "  "+
                        "columnQualifier: " + Bytes.toString(cloneQualifier) + "  " +
                        "columnValue: " + Bytes.toString(cloneValue));
            }
        }
        //  7、关闭表数据
        table.close();
    }

    public static void dropData(String tableName, String rowKey, String columnFamily, String columnName) throws IOException {
        //  1、获取表对象
        Table table = conn.getTable(TableName.valueOf(tableName));
        //  2、构建删除对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //  2-0、构建删除对象(不指定时间戳)
        Delete delWithoutTs = new Delete(Bytes.toBytes(rowKey));
        Delete delWithTs = new Delete(Bytes.toBytes(rowKey),1623828320373L);
        //  2-1、设置删除的列(不加s)
        Delete delColWithoutTs = delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        Delete delColWithTs = delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName),1623775994339L);
        //  2-2、设置删除的列(加s)
        Delete delColWithoutTss = delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        Delete delColWithTss = delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), 1623828706847L);
        // 2-3、设置删除的列族
        Delete delFamily = delete.addFamily(Bytes.toBytes(columnFamily));
        //  3、执行删除操作
        table.delete(delWithoutTs);
        table.delete(delWithTs);
        table.delete(delColWithoutTs);
        table.delete(delColWithTs);
        table.delete(delColWithoutTss);
        table.delete(delColWithTss);
        table.delete(delFamily);
        //  4、关闭连接
        table.close();
    }




    public static void main(String[] args) throws IOException{
        //  1、向表中插入数据
//        putData("literature:Novel", "1001","info1", "bookname","Brothers Karamazov");
//        putData("literature:Novel", "1001","info1", "bookname","Crime and punishment");
//        putData("literature:Novel", "1001","info2", "Authorname","Dostoevsky");
        //  2、获取单行数据
//        getData("literature:Novel","1001","info1","bookname");
        //  3、测试输出多版本数据
//        putData("stu2", "1001","info", "name","Shakespeare");
//        putData("stu2", "1001","info", "name","Goethe");
//        putData("stu2", "1001","info", "name","Balzac");
//        getData("stu2","1001","info","name");
        //  4、测试Scan
//        int count = 1;
//        long startTime = System.currentTimeMillis();
//        for (int i = 0; i < 1000000; i++) {
//            count += i;
//            putData("literature:Novel", String.valueOf(count),"info1", "bookname", String.valueOf(count));
//        }
//        long endTime = System.currentTimeMillis();
//        System.out.println("put10000条数据需要的时间：" + (endTime - startTime));
//        scanTable("literature:Novel");


        //  5、测试删除数据
        dropData("movie","1005","info1","director");

        DdlOperate.close();
    }


}
