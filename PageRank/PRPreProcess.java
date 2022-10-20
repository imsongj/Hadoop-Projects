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

public class PRPreProcess {
    public static enum NodeCounter{COUNT};
    public static class PreprocessMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString());
            int node1 = Integer.parseInt(itr.nextToken());
            int node2 = Integer.parseInt(itr.nextToken());
            context.write(new IntWritable(node1), new IntWritable(node2));
        }
    }

    public static class PreprocessReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        HashMap<Integer, Integer> nodeMap = new HashMap<Integer, Integer>();
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            nodeMap.put(key.get(), 1);
            String output1 = Integer.toString(key.get()) + " 0 ";
            int countNeighbors = 0;
            String output2 = "";
            for(IntWritable val : values){
                int neighborNodeID = val.get();
                countNeighbors++;
                output2 = output2 + " " + Integer.toString(neighborNodeID);
                if(!nodeMap.containsKey(neighborNodeID)){
                    nodeMap.put(neighborNodeID, 0);
                }
            }
            String outputFinal = output1 + Integer.toString(countNeighbors) + output2;
            context.getCounter(NodeCounter.COUNT).increment(1);
            context.write(key, new Text(outputFinal));
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Integer> node : nodeMap.entrySet()){
                int nodeID = node.getKey();
                int emitted = node.getValue();
                if(emitted == 0){
                    context.getCounter(NodeCounter.COUNT).increment(1);
                    String output = Integer.toString(nodeID) + " 0 0";
                    context.write(new IntWritable(nodeID), new Text(output));
                }
            }
        }
    }

/*    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PR PreProcess");

        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PreprocessMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(PreprocessReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class); 
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/
}
