//package part1;

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

public class PDPostProcess {
    
    public static class PostprocessMapper extends Mapper<Object, Text, IntWritable, Text> {
        
        public static int MAX_INT = 2147000000;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            String tmp = itr.nextToken();
            int nodeID = Integer.parseInt(itr.nextToken());
            int distance = Integer.parseInt(itr.nextToken());
            int parentNodeID = Integer.parseInt(itr.nextToken());

            if(distance < MAX_INT){
                String outputStr = Integer.toString(distance);
                if(parentNodeID == -1){
                    outputStr = outputStr + " nil";
                }else{
                    outputStr = outputStr + " " + Integer.toString(parentNodeID);
                }
                context.write(new IntWritable(nodeID), new Text(outputStr));
            }
        }
    }
}
