package hdfstohbase;/*
    @author wxg
    @date 2021/6/17-10:44
    */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitDriver implements Tool {
    //定义一个Configuration
    private Configuration conf = null;

    @Override
    public int run(String[] args) throws Exception {
         //   1、获取job对象
        Job job = Job.getInstance(conf);
        //  2、设置驱动类路径
        job.setJarByClass(FruitDriver.class);
        //  3、设置Mapper以及Mapper输出的KV类型
        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        //  4、设置Reducer类(★★★★★)
        TableMapReduceUtil.initTableReducerJob(args[1],FruitReducer.class, job);
        //  5、设置输入参数
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //  6、提交任务
        boolean result = job.waitForCompletion(true);
        //  7、返回结果
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
        Configuration conf = new Configuration();
        int run = 0;
        try {
            run = ToolRunner.run(conf, new FruitDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(run);
    }
}
