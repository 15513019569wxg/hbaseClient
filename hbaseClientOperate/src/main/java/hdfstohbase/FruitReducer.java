package hdfstohbase;/*
    @author wxg
    @date 2021/6/17-10:45
    */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {
//    String cf1 = null;
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        Configuration configuration = context.getConfiguration();
//        String cf1 = configuration.get("cf1");
//    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //  1、遍历values: 1001    apple   Red
        for (Text value : values) {
            //  2、获取每一行数据
            String[] fields = value.toString().split("\t");
            //  3、构建put对象
            Put put = new Put(Bytes.toBytes(fields[0]));
            //  4、给对象赋值,这些参数都是可以动态传入的，利用setup()方法
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(fields[2]));
            //  5、写出
            context.write(NullWritable.get(),put);
        }
    }
}
