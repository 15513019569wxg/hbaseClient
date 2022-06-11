package habsetohbase;/*
    @author wxg
    @date 2021/6/17-14:07
    */


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadFromHbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //构建put对象
        Put put = new Put(key.get());
        //  1、获取数据(这里的数据包括的是每个不同的rowkey)
        for (Cell cell : value.rawCells()) {
            //  2、判断当前的cell是否为“name”列
            if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                //  3、给Put对象赋值
                put.add(cell);
            }
        }

        //  4、写出
        context.write(key, put);
    }
}
