package utils;/*
    @author wxg
    @date 2021/6/18-16:56
    */


import constants.Constants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBaseUtil {
    /**
     * 创建命名空间
     * @param nameSpace
     * @throws IOException
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        //  1、Hbase的配置信息
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //  2、获取Admin对象
        Admin admin = connection.getAdmin();
        //  3、构建命名空间构造器
        NamespaceDescriptor build = NamespaceDescriptor.create(nameSpace).build();
        //  4、创建命名空间
        admin.createNamespace(build);
        //  5、关闭资源
        admin.close();
        connection.close();
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return exists
     */
    private static boolean isTableExists(String tableName) throws IOException {
        //  1、获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //  2、获取Admin对象
        Admin admin = connection.getAdmin();
        //  3、判断是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //  4、关闭资源
        admin.close();
        connection.close();
        //  5、返回结果
        return exists;
    }

    /**
     * 创建表
     * @param tableName
     * @param versions
     * @param cfs
     * @throws IOException
     */
    public static void createTable(String tableName, int versions, String...cfs) throws IOException {
        //  1、判断是否传入了列族信息
        if(cfs.length < 0){
            System.out.println("请设置列族信息");
        }
        //  2、判断表是否存在
        if(isTableExists(tableName)){
            System.out.println(tableName + "表已经存在");
        }
        //  3、获取Connection对象
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //  4、获取Admin对象
        Admin admin = connection.getAdmin();
        //  5、创建表描述器的构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        //  6、循环添加列族信息
        for (String cf : cfs) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                    //  7、创建构造器
                    .newBuilder(cf.getBytes())
                    //  8、设置版本
                    .setMaxVersions(versions)
                    //  9、生成列族描述器
                    .build();
            //  10、添加列族描述器
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        //  11、生成表描述器
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        //  12、创建表
        admin.createTable(tableDescriptor);
        //  13、关闭资源
        admin.close();
        connection.close();
    }


}
