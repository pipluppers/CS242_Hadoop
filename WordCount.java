import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper< KEYIN, VALUEIN, KEYOUT, VALUEOUT >
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	// Create an IntWritable (basically an integer) with the value 1
	private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

	// Called once for each key-value pair in the input split
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// StringTokenizer takes in a string (e.g. value.toString)
		// Each token is a word (space-separated)
		// Basically, using StringTokenizer and nextToken() works like the split function with strings in 
		// 	the regex library
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

	// Grabs the key and all of the values associated with that key
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

		// Loops through all of the values
            for (IntWritable val : values) {
		// Use val.get() to get the value (i.e. integer) of an IntWritable
                sum += val.get();
            }

		// Assign the integer sum to the IntWritable result
            result.set(sum);

		// Emit the key-value pair
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}