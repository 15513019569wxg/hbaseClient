package hbaseclient;/*
    @author wxg
    @date 2021/6/15-15:16
    */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;



import java.io.IOException;

public class DdlOperate {
    private static  Connection conn = null;
    private static Admin admin = null;
    static {
        try {
            //  1、获取配置信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104", "src/src/main/resources/hbase-site.xml");
            //  2、创建连接对象
            conn = ConnectionFactory.createConnection(configuration);
            //  3、创建Admin对象
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("===============================开始测试==============================");
    }

    public DdlOperate() {
    }

    public static Admin getAdmin() {
        return admin;
    }

    public static Connection getConn() {
        return conn;
    }

    /**
     * @describe 方法一：判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
   public static boolean isTableExist(String tableName) throws IOException {
       return admin.tableExists(TableName.valueOf(tableName));
   }
    /**
     * @describe 方法二：创建表存在
     * @param tableName
     * @param columnNames
     * @throws IOException
     * @throws DeserializationException
     */
    public static void createTable(String tableName, String...columnNames) throws IOException, DeserializationException {
        //  1、判断是否存在列族信息
        if(columnNames.length <= 0){
            System.out.println("请设置相应的列族信息");
            return;
        }
        //  2、判断表是否存在
        if(isTableExist(tableName)){
            System.out.println(tableName + "已经存在，创建失败！！！");
            return;
        }
        //  3、创建表描述构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        //  4、循环添加列族信息
        for (String columnName : columnNames) {
            //  5、使用列族描述构造器创建列族描述器
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(columnName.getBytes()).build();
            //  6、往表描述构造器中添加列族描述器
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        //  7、生成完整的表描述器
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //  8、创建表
        admin.createTable(tableDescriptor);

    }
    /**
     * @describe 方法三:删除表
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        //  1、判断表是否存在
        if (!isTableExist(tableName)){
            System.out.println(tableName + "表不存在！！！");
        }
        //  2、获取表
        TableName tableName1 = TableName.valueOf(tableName);
        //  3、使表下线
        admin.disableTable(tableName1);
        //  4、删除表
        admin.deleteTable(tableName1);
        System.out.println("删除表成功");
    }
    /**
     * @describe 方法四:创建命名空间
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace){
        //  1、创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //  2、创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e){
            System.out.println("该命名空间已存在");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 关闭连接
     */
    public static void close() {
        if(conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println("==========================成功关闭连接===============================");

    }


    public static void main(String[] args) throws IOException, DeserializationException {
        //  1、测试表是否存在
        System.out.println(isTableExist("shuihu"));
        //  2、创建表测试
        //createTable("shuihu","info1","info2");
        //createTable("sanguo","info1","info2");
        //  3、验证创建的表是否存在
        //System.out.println(isTableExist("shuihu"));
        //  4、删除表
        //dropTable("sanguo");
        //  5、创建命名空间
        //createNameSpace("literature");
        //createTable("literature:Novel","info1","info2");
        //  关闭资源
        close();


    }
}
