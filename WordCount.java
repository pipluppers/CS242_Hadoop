import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*

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
	// Empty word
        private Text word = new Text();

	// Called once for each key-value pair in the input split
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// StringTokenizer takes in a string (e.g. value.toString)
		// Each token is a word (space-separated)
		// Basically, using StringTokenizer and nextToken() works like the split function with strings in 
		// 	the regex library
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
		// Write the token to the word
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

/*
	//	Input Key:	Object KEY
	//	Input Value: 	Text JSON
	//	Output Key: 	Text Hashtag
	//	Output Value: 	Text Tweet
	public static class TweetMapper extends Mapper<Object, Text, Text, Text> {
		
		//	Create a Text variable with nothing
		private Text word = new Text();

		// Value will be the tweet JSON
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Extract the hashtag from value
			// Extract the tweet text from value
			// context.write(hashtag, tweet);
		}
	}

*/

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

/*
	// Input key:	Text Hashtag
	// Input value:	Text Tweet
	// Output key:	Text Hashtag
	// Ouput value:	Iterable<Text> Tweets
	public static class TweetReducer extends Reducer<Text, Text, Text, Iterable<Text>> {
		private Iterable<Text> res = new Iterable<Text>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> tweetList = new ArrayList<String>();
			int x = 0;
			for (Text val : values) {
				tweetList.add(x, val);
				x = x + 1;
			}
			
			// Add everything in tweetList to res
			
			
			// Emit
			context.write(key, res);
		}
	}

*/


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
