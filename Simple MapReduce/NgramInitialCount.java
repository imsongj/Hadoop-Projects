import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramInitialCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {

        String[] prev_n_initials;
        int N;
        HashMap<String, HashMap<String, Integer>> first_inital_map = new HashMap<String, HashMap<String, Integer>>();

        public void setup(Context context){
            Configuration conf = context.getConfiguration(); //use this to pass parameter
            N = Integer.parseInt(conf.get("ngram"));
            prev_n_initials = new String[N - 1];
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^A-Za-z]", " "));
            int num_initials = itr.countTokens() + N - 1;
            String[] initials = new String[num_initials];
            
            for(int i = 0; i < N - 1; i++){
                initials[i] = prev_n_initials[i];
            }
            int idx = N - 1;
            while (itr.hasMoreTokens()) {
                initials[idx] = itr.nextToken().substring(0, 1); //j o ....
                idx = idx + 1;
            }
            for(int i = num_initials - (N - 1), j = 0; j < N - 1; i++, j++){ 
                prev_n_initials[j] = initials[i];
            }
            int i, j;
            String initial_list = "";
            for (i = 0; i <= num_initials - N; i++) { 
                if(initials[i] != null){
                    String key_initial = initials[i]; // T, qr, 1
                    HashMap<String, Integer> inner_map = new HashMap<String, Integer>();
                    if (first_inital_map.containsKey(key_initial)) {
                        inner_map = first_inital_map.get(key_initial);
                    }
                    for (j = i + 1; j < i + N; j++) {
                        initial_list = initial_list + initials[j]; //key_initial + initial_list = ngram 
                    }
                    if (inner_map.containsKey(initial_list)) {
                        inner_map.put(initial_list, (int) inner_map.get(initial_list) + 1);
                    } else {
                        inner_map.put(initial_list, 1);
                    }
                    first_inital_map.put(key_initial, inner_map);
                    initial_list = "";
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, HashMap<String, Integer>> outer_entry : first_inital_map.entrySet()) {
                String first_inital = outer_entry.getKey();
                MapWritable map = new MapWritable();
                for (Map.Entry<String, Integer> inner_entry : outer_entry.getValue().entrySet()) {
                    String initial_list = inner_entry.getKey();
                    Integer count = inner_entry.getValue();
                    map.put(new Text(initial_list), new IntWritable(count));
                    // ... T, qb, 1
                }
                context.write(new Text(first_inital), map);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {

            MapWritable final_map = new MapWritable();
            for (MapWritable val : values) {
                for (Map.Entry<Writable, Writable> map : val.entrySet()) {
                    Text initial_list = (Text)map.getKey();
                    IntWritable count = (IntWritable)map.getValue();
                    if(final_map.containsKey(initial_list)){
                        final_map.put(initial_list, new IntWritable(((IntWritable)final_map.get(initial_list)).get() + count.get()));
                    }else{
                        final_map.put(initial_list, count);
                    }
                }
            }
            for (Map.Entry<Writable, Writable> map : final_map.entrySet()) {
                Text initial_list = (Text)map.getKey();
                IntWritable count = (IntWritable)map.getValue();
                context.write(new Text(key.toString() + " " + initial_list.toString().replace("", " ").trim()), count);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("ngram", args[2]);
        Job job = Job.getInstance(conf, "Ngram Initial Count");
        job.setJarByClass(NgramInitialCount.class);
        job.setMapperClass(TokenizerMapper.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
