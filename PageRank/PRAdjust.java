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

public class PRAdjust {
    
    public static class AdjustMapper extends Mapper<Object, Text, IntWritable, Text> {

        double mass;
        double alpha;
        double g;
        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter
            mass = Double.parseDouble(conf.get("mass"));
            alpha = Double.parseDouble(conf.get("alpha"));
            g = Double.parseDouble(conf.get("numNode"));
        }    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            PRNodeWritable currentNode = new PRNodeWritable();

            StringTokenizer itr = new StringTokenizer(value.toString()); //fromString
            itr.nextToken(); //node ID
            currentNode.setNodeID(Integer.parseInt(itr.nextToken()));
            double pagerank = Double.parseDouble(itr.nextToken());
            int size = Integer.parseInt(itr.nextToken());
            for(int i = 0; i < size; ++i){ //for all neighbor of current node
                int neighborNodeID = Integer.parseInt(itr.nextToken());
                currentNode.addNeighbor(neighborNodeID);
            }
            double adjustedPagerank = (alpha * (1.0 / g)) + ((1 - alpha) * ((mass / g) + pagerank));
            currentNode.setPagerank(adjustedPagerank);
            context.write(new IntWritable(currentNode.getNodeID()), new Text(currentNode.toString()));
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
