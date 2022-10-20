//package part1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapred.Counters;

public class ParallelDijkstra {
    public static enum NodeCounter{COUNT};
    public static class DijkstraMapper extends Mapper<LongWritable, Text, IntWritable, PDNodeWritable> {

        String option;
        int srcID;

        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter
            option = conf.get("option");
            srcID = Integer.parseInt(conf.get("src"));
        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            PDNodeWritable currentNode = new PDNodeWritable();

            StringTokenizer itr = new StringTokenizer(value.toString()); //fromString
            itr.nextToken(); //node ID
            currentNode.setNodeID(Integer.parseInt(itr.nextToken()));
            int distance = Integer.parseInt(itr.nextToken());
            if(currentNode.getNodeID() == srcID) distance = 0;
            currentNode.setDistance(distance);
            currentNode.setParentNodeID(Integer.parseInt(itr.nextToken()));
            int size = Integer.parseInt(itr.nextToken());
            
            for(int i = 0; i < size; ++i){ //for all neighbor of current node
                int neighborNodeID = Integer.parseInt(itr.nextToken());
                int weight = Integer.parseInt(itr.nextToken());
                currentNode.addNeighbor(neighborNodeID, weight);
                int newDistance;
                if(option.equals("weighted")){
                    newDistance = distance + weight;
                }else{
                    newDistance = distance + 1;
                }
                
                PDNodeWritable tmp = new PDNodeWritable(-2, newDistance, currentNode.getNodeID());
                context.write(new IntWritable(neighborNodeID), tmp); //emit updated distance and parent node
            }
            context.write(new IntWritable(currentNode.getNodeID()), currentNode);
        }
    }

    public static class DijkstraReducer extends Reducer<IntWritable, PDNodeWritable, IntWritable, Text> {

        public static int MAX_INT = 2147000000;
        
        int srcID;
        
        public void setup(Context context){
            Configuration conf = context.getConfiguration(); 
            srcID = Integer.parseInt(conf.get("src"));
        }
        public void reduce(IntWritable key, Iterable<PDNodeWritable> values, Context context)
                throws IOException, InterruptedException {
            int minDistance = MAX_INT;
            int minParentID = -1; 
            PDNodeWritable node = new PDNodeWritable();

            
            for (PDNodeWritable d : values) {
                if(key.get() == d.getNodeID()){ //if d is the passed on node struct
                    node = new PDNodeWritable(d); //deep copy
                }
                if(d.getDistance() < minDistance){
                    minDistance = d.getDistance();
                    minParentID = d.getParentNodeID();
                }
            }
            int prevDist = node.getDistance();
            if(prevDist != minDistance){
                context.getCounter(NodeCounter.COUNT).increment(1);
            }
            if(minDistance < MAX_INT) node.setDistance(minDistance);
            if(minParentID != -1) node.setParentNodeID(minParentID);
            if(node.getNodeID() == -2) node.setNodeID(key.get());
            
            context.write(key, new Text(node.toString()));
            
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
		conf1.set("src", args[2]);
        conf1.set("iterations", args[3]);
        conf1.set("option", args[4]);

		Job job1 = Job.getInstance(conf1,"preprocess");
		job1.setJarByClass(ParallelDijkstra.class);
		job1.setJar("pd.jar");
        job1.setMapperClass(PDPreProcess.PreprocessMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(PDPreProcess.PreprocessReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class); 
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
		Path output = new Path(args[1]);
        String tmpPath = output.getParent().toString() + "/tmp";
		Path outputJob1 = new Path(tmpPath + "/tmp0");
		FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, outputJob1);
		FileSystem fs = FileSystem.get(conf1);
		if (fs.exists(outputJob1)) {
			System.out.println("--------deleted folder: " + "/tmp0");
			fs.delete(outputJob1, true);
		}
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        int i = 0;
		if(job1.waitForCompletion(true)) {
			System.out.println("--------job1 finished");
			i = 0;
			int iteration = Integer.parseInt(args[3]);
			long flag = 1;
			while(true){
				if(iteration == 0){
					if(flag == 0) {
						System.out.println("--------job2 finished");
						break;
					}
				}else{
					if(flag == 0 || i == iteration){
						System.out.println("--------job2 finished");
						break;
					} 
				}
				
				Job job2 = Job.getInstance(conf1, "Parallel Dijkstra");
				job2.setJarByClass(ParallelDijkstra.class);
				job2.setJar("pd.jar");

				job2.setMapperClass(ParallelDijkstra.DijkstraMapper.class);
				// job.setCombinerClass(IntSumReducer.class);
				job2.setReducerClass(ParallelDijkstra.DijkstraReducer.class);

				job2.setMapOutputKeyClass(IntWritable.class);
				job2.setMapOutputValueClass(PDNodeWritable.class); 
				job2.setOutputKeyClass(IntWritable.class);
				job2.setOutputValueClass(Text.class);
				Path outputJob2 = new Path(tmpPath + "/tmp" + Integer.toString(i + 1));

				FileInputFormat.addInputPath(job2, new Path(tmpPath + "/tmp" + Integer.toString(i)));
				FileOutputFormat.setOutputPath(job2, outputJob2);
				fs = FileSystem.get(conf1);
				if (fs.exists(outputJob2)) {
					System.out.println("--------deleted folder: /tmp" + Integer.toString(i + 1));
					fs.delete(outputJob2, true);
				}
				if(job2.waitForCompletion(true)) {;
					System.out.println("--------# of iteration: " + Integer.toString(i + 1));
					flag = job2.getCounters().findCounter(ParallelDijkstra.NodeCounter.COUNT).getValue();
					System.out.println("--------# of newly found nodes: " + Long.toString(flag));
				}else {
					System.out.println("--------failed");
				}	
				i = i + 1;
			}
		}
        Job job3 = Job.getInstance(conf1, "PD PostProcess");
		
        job3.setJarByClass(ParallelDijkstra.class);
		job3.setJar("pd.jar");
        job3.setMapperClass(PDPostProcess.PostprocessMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
		Path outputJob3 = new Path(args[1]);
        FileInputFormat.addInputPath(job3, new Path(tmpPath + "/tmp" + Integer.toString(i)));
        FileOutputFormat.setOutputPath(job3, outputJob3);

		if (fs.exists(outputJob3)) {
					System.out.println("--------deleted folder: /final");
					fs.delete(outputJob3, true);
				}
		if(job3.waitForCompletion(true)){
			System.out.println("--------job3 finished, final result in output/");
			System.exit(0);
		}else{
			System.exit(1);
		}
	}
}
