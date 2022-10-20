//package part2;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class PageRank {
    public static enum MassCounter{COUNT};
    public static class PRMapper extends Mapper<LongWritable, Text, IntWritable, PRNodeWritable> {

        String isFirstItr;
        double initP;

        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter
            isFirstItr = conf.get("isFirstItr");
            initP = 1/(Double.parseDouble(conf.get("numNode")));

        }
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            PRNodeWritable currentNode = new PRNodeWritable();

            StringTokenizer itr = new StringTokenizer(value.toString()); //fromString
            itr.nextToken(); //node ID
            currentNode.setNodeID(Integer.parseInt(itr.nextToken()));
            double pagerank = Double.parseDouble(itr.nextToken());
            if(isFirstItr.equals("true")){ //1st iteration
                PRNodeWritable tmp = new PRNodeWritable(-2, initP);
                context.write(new IntWritable(currentNode.getNodeID()), tmp);
                int size = Integer.parseInt(itr.nextToken());
                for(int i = 0; i < size; ++i){ //for all neighbor of current node
                    int neighborNodeID = Integer.parseInt(itr.nextToken());
                    currentNode.addNeighbor(neighborNodeID);
                }
            }else{ //2nd~ iteration
                currentNode.setPagerank(pagerank);
                int size = Integer.parseInt(itr.nextToken());
                for(int i = 0; i < size; ++i){ //for all neighbor of current node
                    int neighborNodeID = Integer.parseInt(itr.nextToken());
                    currentNode.addNeighbor(neighborNodeID);
                    PRNodeWritable tmp = new PRNodeWritable(-2, pagerank/size);
                    context.write(new IntWritable(neighborNodeID), tmp);
                }
            }
            context.write(new IntWritable(currentNode.getNodeID()), currentNode);
        }
    }

    public static class PRReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, Text> {
        
        public double mass = 0;

        public void setup(Context context){
            Configuration conf = context.getConfiguration(); 

        }
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context)
                throws IOException, InterruptedException {

            PRNodeWritable node = new PRNodeWritable();

            double sum = 0;
            for (PRNodeWritable d : values) {
                if(key.get() == d.getNodeID()){ //if d is the passed on node struct
                    node = new PRNodeWritable(d); //deep copy
                }else{
                    sum += d.getPagerank();
                }

            }
            node.setPagerank(sum);
            mass += sum;
            context.write(key, new Text(node.toString()));
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            mass = 1 - mass;
            context.getCounter(MassCounter.COUNT).increment((long)(mass * 1e18));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf1 = new Configuration();
		conf1.set("alpha", args[0]);
        conf1.set("iterations", args[1]);

		Job job1 = Job.getInstance(conf1,"preprocess");
		job1.setJarByClass(PageRank.class);
		job1.setJar("pr.jar");
        job1.setMapperClass(PRPreProcess.PreprocessMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(PRPreProcess.PreprocessReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class); 
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
		
		Path output = new Path(args[3]);
        String tmpPath = output.getParent().toString() + "/tmp";
		Path outputJob1 = new Path(tmpPath + "/tmp0");
		FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, outputJob1);
		FileSystem fs = FileSystem.get(conf1);
		if (fs.exists(outputJob1)) {
			fs.delete(outputJob1, true);
		}
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);
        
		if(job1.waitForCompletion(true)) {
			System.out.println("--------job1 finished");

			Long job1Counter = job1.getCounters().findCounter(PRPreProcess.NodeCounter.COUNT).getValue();
			Configuration conf2 = new Configuration();
			conf2.set("numNode", Long.toString(job1Counter));
			conf2.set("isFirstItr", "true");
			int iteration = Integer.parseInt(args[1]);
			for(int i = 0; i < iteration * 2; i = i + 2){
				Job job2 = Job.getInstance(conf2, "PageRank");
				job2.setJarByClass(PageRank.class);
				job2.setJar("pr.jar");

				job2.setMapperClass(PageRank.PRMapper.class);
				// job.setCombinerClass(IntSumReducer.class);
				job2.setReducerClass(PageRank.PRReducer.class);

				job2.setMapOutputKeyClass(IntWritable.class);
				job2.setMapOutputValueClass(PRNodeWritable.class); 
				job2.setOutputKeyClass(IntWritable.class);
				job2.setOutputValueClass(Text.class);
				Path outputPageRank = new Path(tmpPath + "/tmp" + Integer.toString(i + 1));

				FileInputFormat.addInputPath(job2, new Path(tmpPath + "/tmp" + Integer.toString(i)));
				FileOutputFormat.setOutputPath(job2, outputPageRank);

				if (fs.exists(outputPageRank)) {
					fs.delete(outputPageRank, true);
				}
				if(job2.waitForCompletion(true)) {;
					System.out.println("--------# of iteration: " + Integer.toString(i/2 + 1));
				}else {
					System.out.println("--------failed");
				}
				Long job2Counter = job2.getCounters().findCounter(PageRank.MassCounter.COUNT).getValue();
				double mass = job2Counter / 1e18;
				conf2.set("isFirstItr", "false");

				Configuration conf3 = new Configuration();
				conf3.set("mass", Double.toString(mass));
				conf3.set("alpha", args[0]);
				conf3.set("numNode", Long.toString(job1Counter));

				Job job3 = Job.getInstance(conf3, "PR Adjust");

				job3.setJarByClass(PageRank.class);
				job3.setJar("pr.jar");
				job3.setMapperClass(PRAdjust.AdjustMapper.class);
				job3.setNumReduceTasks(0);
				job3.setOutputKeyClass(IntWritable.class);
				job3.setOutputValueClass(Text.class);
				Path outputAdjust = new Path(tmpPath + "/tmp" + Integer.toString(i + 2));
				FileInputFormat.addInputPath(job3, outputPageRank);
				FileOutputFormat.setOutputPath(job3, outputAdjust);

				if (fs.exists(outputAdjust)) {
					fs.delete(outputAdjust, true);
				}
				if(job3.waitForCompletion(true)){
					System.out.println("--------job2, 3 finished");
				}else{
					System.out.println("--------failed");
				}
			}
			Job job4 = Job.getInstance(conf1, "PR PostProcess");
		
			job4.setJarByClass(PageRank.class);
			job4.setJar("pr.jar");
			job4.setMapperClass(PRPostProcess.PostprocessMapper.class);
			job4.setNumReduceTasks(0);
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);
			Path outputPostProcess = new Path(args[3]);
			FileInputFormat.addInputPath(job4, new Path(tmpPath + "/tmp" + Integer.toString(iteration * 2)));
			FileOutputFormat.setOutputPath(job4, outputPostProcess);

			if (fs.exists(outputPostProcess)) {
				System.out.println("--------deleted folder: output/");
				fs.delete(outputPostProcess, true);
			}
			if(job4.waitForCompletion(true)){
				System.out.println("--------job4 finished, final result in output/");
				System.exit(0);
			}else{
				System.exit(1);
			}
		}
        
	}
}
