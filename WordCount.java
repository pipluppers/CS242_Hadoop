import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class StringArrayWritable extends ArrayWritable {
		// Constructors
		public StringArrayWritable() {
			super(Text.class);
		}
		public StringArrayWritable(Text[] values) {
			super(Text.class, values);

			Text[] t = new Text[values.length];
			for (int i = 0; i < values.length; ++i) {
				t[i] = values[i];
			}
			// Write all texts to StringArrayWritable object
			set(t);
		}
/*
		public String

		@Override
		public String toStrings() {
			Text [] t = to
		}
*/		
	}

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
			//String[] json = value.toString().split("\\s");
			
			// Regex Stuff
			// Currently writing hashtag:text key-value. Want hashtag:[text,loc,screen_name,profile]
 			
			String hashtag = "\"hashtags\": \\[([^\\]]*)\\]";
			String tweet = "\"text\": \"([^\"]*)\"";
			Pattern h = Pattern.compile(hashtag);
			Pattern t = Pattern.compile(tweet);
			Matcher m_hash = h.matcher(value.toString());
			Matcher m_text = t.matcher(value.toString());
			context.write(new Text(m_hash.group(1)), new Text(m_text.group(1)));
			

			
			//context.write(new Text(json[0]), new Text(json[1]));

		}
	}


	// Input key:	Text Hashtag
	// Input value:	Text Tweet
	// Output key:	Text Hashtag
	// Ouput value: StringArrayWritable Tweets
	public static class TweetReducer extends Reducer<Text, Text, Text, StringArrayWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<String> texts = new ArrayList<String>();

			for (Text val : values) {
				texts.add(val.toString());
			}
			Text[] t = new Text[texts.size()];
			for (int i = 0; i < texts.size(); ++i) {
				t[i] = new Text(texts.get(i));
			}
			
			// Emit
			context.write(key, new StringArrayWritable(t));
		}
	}



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        
	// New
	job.setMapperClass(TweetMapper.class);
	job.setCombinerClass(TweetReducer.class);
	job.setReducerClass(TweetReducer.class);
        job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(StringArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
