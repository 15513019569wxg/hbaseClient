package habsetohbase;/*
    @author wxg
    @date 2021/6/17-14:07
    */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseToHbaseDriver implements Tool {
    //定义一个Configuration
    private Configuration conf = null;

    @Override
    public int run(String[] args) throws Exception {
        //   1、获取job对象
        Job job = Job.getInstance(conf);
        //  2、设置驱动类路径
        job.setJarByClass(HbaseToHbaseDriver.class);
        //  3、设置Mapper以及输出KV类型
        TableMapReduceUtil.initTableMapperJob(
                                        "fruit",
                                        new Scan(),
                                        ReadFromHbaseMapper.class,
                                        ImmutableBytesWritable.class,
                                        Put.class,
                                        job);
        //  4、设置Reducer以及输出的表
        TableMapReduceUtil.initTableReducerJob("fruit14", WriteToHbaseReducer.class, job);
        //  5、提交任务
        boolean result = job.waitForCompletion(true);
        //  6、返回结果
        return result? 0 : 1;

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        try {
            ToolRunner.run(configuration, new HbaseToHbaseDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
