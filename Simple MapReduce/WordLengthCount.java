import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        Map word_cnt = new HashMap<Integer, Integer>();
        IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                int word_len = word.length();

                if (word_cnt.containsKey(word_len)) {
                    word_cnt.put(word_len, (int)word_cnt.get(word_len) + 1);
                } else {
                    word_cnt.put(word_len, 1);
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {

            Iterator<Map.Entry<Integer, Integer>> temp = word_cnt.entrySet().iterator();

            while (temp.hasNext()) {
                Map.Entry<Integer, Integer> entry = temp.next();
                Integer key = entry.getKey();
                Integer val = entry.getValue();

                context.write(new IntWritable(key), new IntWritable(val));
            }
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word length count");
        job.setJarByClass(WordLengthCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
