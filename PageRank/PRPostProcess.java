//package part2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.Counters;

public class PRPostProcess {
    
    public static class PostprocessMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString()); //fromString
            itr.nextToken(); //node ID
            String nodeID = itr.nextToken();
            String pagerank = itr.nextToken();
            
            context.write(new Text(nodeID), new Text(pagerank));
        }
    }

    /*public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job3 = Job.getInstance(conf, "PD PostProcess");

        job3.setJarByClass(PRAdjust.class);
        job3.setMapperClass(PostprocessMapper.class);
        job3.setReducerClass(PostprocessReducer.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/
}
