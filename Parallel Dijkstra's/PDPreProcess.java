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

public class PDPreProcess {
    
    public static class PreprocessMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            int node1 = Integer.parseInt(itr.nextToken());
            int node2 = Integer.parseInt(itr.nextToken());
            int weight = Integer.parseInt(itr.nextToken());
            String output = Integer.toString(node2) + " " + Integer.toString(weight);
            context.write(new IntWritable(node1), new Text(output));
        }
    }

    public static class PreprocessReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            String output1 = Integer.toString(key.get()) + " " + "2147000000 -1 ";
            int countNeighbors = 0;
            String output2 = "";
            for(Text val : values){
                countNeighbors++;
                StringTokenizer itr = new StringTokenizer(val.toString());
                int neighborNodeID = Integer.parseInt(itr.nextToken());
                int weight = Integer.parseInt(itr.nextToken());
                output2 = output2 + " " + Integer.toString(neighborNodeID) + " " + Integer.toString(weight);
            }
            String outputFinal = output1 + Integer.toString(countNeighbors) + output2;
            context.write(key, new Text(outputFinal));
        }
    }
}
