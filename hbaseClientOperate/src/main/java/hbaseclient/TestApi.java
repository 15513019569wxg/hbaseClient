package hbaseclient;/*
    @author wxg
    @date 2021/6/15-11:01
    */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class TestApi {
    public static boolean isTableExist(TableName tableName) throws IOException {
        // 1、获取配置文件
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104", "src/src/main/resources/hbase-site.xml");
        // 2、获取连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 3、获取管理员对象
        Admin admin = connection.getAdmin();
        // 4、判断表是否存在
        boolean isTableExist = admin.tableExists(tableName);
        // 5、关闭连接
        admin.close();
        // 6、返回结果
        return isTableExist;
    }


    public static void main(String[] args) throws IOException {
        // 1、测试表是否存在
//        boolean tableExist = isTableExist(TableName.valueOf("stu2"));
        boolean tableExist = isTableExist(TableName.valueOf("teacher"));
        System.out.println(tableExist);
    }
}
