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
		public StringArrayWritable() {
			super(Text.class);
		}
		public StringArrayWritable(String[] s) {
			super(Text.class);
			Text[] txts = new Text[s.length];
			for (int i = 0; i < s.length; ++i) {
				txts[i] = new Text(s[i]);
			}
			set(txts);
		}
	}

	/*
	public static class StringArrayWritable extends ArrayWritable {
		// Constructors
		public StringArrayWritable() {
			super(Text.class);
		}
		public StringArrayWritable(List<String> l) {
			super(Text.class);
			Text[] t = new Text[l.size()];
			for (int i = 0; i < l.size(); ++i) {
				t[i] = new Text(l.get(i));
			}
			set(t);
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

//		public String

//		@Override
	//	public String toStrings() {
	//		Text [] t = to
	//	}
		
	}
*/
	//	Input Key:	Object KEY
	//	Input Value: 	Text JSON
	//	Output Key: 	Text Hashtag
	//	Output Value: 	Text Tweet      OR ArrayWritable TweetInfo (NEW)
//	public static class TweetMapper extends Mapper<Object, Text, Text, Text> {
	public static class TweetMapper extends Mapper<Object, Text, Text, StringArrayWritable> {	// NEW
		
		// Value will be the tweet JSON
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Extract the hashtag from value
			//String[] json = value.toString().split("\\s");
	
			// TODO
			// File is in the format [{TWEET},{TWEET},...]
			// How to separate all of these
		
			// Regex Stuff
			// Currently writing hashtag:text key-value. Want hashtag:[text,loc,screen_name,profile]
			//List<String> tweet_info = new ArrayList<String>();			
			String[] tweet_info = new String[3];
			String hashtag = "\"hashtags\": \\[([^\\]]*)\\]";
			String tweet = "\"text\": \"([^\"]*)\"";
			String location = "\"location\": \"([^\"]*)\"";
			String profile_pic = "\"profile_image_url\": \"([^\"]*)\"";

			Pattern h = Pattern.compile(hashtag);
			Pattern t = Pattern.compile(tweet);
			Pattern loc = Pattern.compile(location);
			Pattern prof = Pattern.compile(profile_pic);

			Matcher m_hash = h.matcher(value.toString());
			Matcher m_text = t.matcher(value.toString());
			Matcher m_loc = loc.matcher(value.toString());
			Matcher m_prof = prof.matcher(value.toString());
			
			while(m_hash.find()) hashtag = m_hash.group(1);
			while(m_text.find()) tweet = m_text.group(1);
			while(m_loc.find()) location = m_loc.group(1);
			while(m_prof.find()) profile_pic = m_prof.group(1);
			tweet_info[0] = tweet; tweet_info[1] = location; tweet_info[2] = profile_pic;
			context.write(new Text(hashtag), new StringArrayWritable(tweet_info));	// NEW
		}
	}


	// Input key:	Text Hashtag
	// Input value:	Text Tweet		OR StringArrayWritable TweetInfo (NEW)
	// Output key:	Text Hashtag
	// Ouput value: StringArrayWritable Tweets
//	public static class TweetReducer extends Reducer<Text, Text, Text, ArrayWritable> {
//	public static class TweetReducer extends Reducer<Text, ArrayWritable, Text, ArrayWritable> {	// NEW
	public static class TweetReducer extends Reducer<Text, StringArrayWritable, Text, StringArrayWritable> {	// NEW2

		public void reduce(Text key, Iterable<StringArrayWritable> values, Context context) throws IOException, InterruptedException {
//		public void reduce(Text key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException{

			// NEW part
			// listoflist[0] is tweet, listoflist[1] is location, listoflist[2] is profilepicurl
			/*
 			ArrayList<ArrayList<String>> listoflist = new ArrayList<ArrayList<String>>(3);
			ArrayList<String> innerStr = new ArrayList<String>(3);
  			*/

			String[] str = new String[3];
			String[] res = new String[3];
			res[0] = ""; res[1] = ""; res[2] = "";
/*
			for (Text val : values) {
				texts.add(val.toString());
			}
*/
//			for (ArrayWritable val : values) {
			for (StringArrayWritable val:values) {
				str = val.toStrings();
				res[0] = res[0] + str[0]; res[1] = res[1] + str[1]; res[2] = res[2] + str[2];
			}
		
//			context.write(key, new ArrayWritable(res));
			context.write(key, new StringArrayWritable(res));

/*
			String[] placeholder = new String[1];
			placeholder[0] = "hi";
			Text[] t = new Text[texts.size()];
			for (int i = 0; i < texts.size(); ++i) {
				t[i] = new Text(texts.get(i));
			}
			
			// Emit
			context.write(key, new ArrayWritable(placeholder));
*/
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
//	job.setOutputValueClass(ArrayWritable.class);
	job.setOutputValueClass(StringArrayWritable.class);	// NEW

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
