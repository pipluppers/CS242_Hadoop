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
		public String[] toString() {
			String[] new_str = new String[this.length];
		}
		
	}

	//	Input Key:	Object KEY
	//	Input Value: 	Text JSON
	//	Output Key: 	Text Hashtag
	//	Output Value: 	Text Tweet      OR ArrayWritable TweetInfo (NEW)
//	public static class TweetMapper extends Mapper<Object, Text, Text, Text> {
	public static class TweetMapper extends Mapper<Object, Text, Text, Text> {	// NEW
		
		// Value will be the tweet JSON
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// TODO
			// File is in the format [{TWEET},\n{TWEET},...]
			// How to separate all of these
		
			// Regex Stuff
			String tweet = "";	// Should be our output value
			String tweetjson, user, content, name, screenname, location;
			content = name = screenname = location = "";
			Pattern p_tweetjsons = Pattern.compile("\\{(.*?\"lang\": \".*?(?=\")\"\\})"); //{ANYTHNIG, "lang": "any"}
			Matcher m_tweetjsons = p_tweetjsons.matcher(value.toString());
			Pattern p_hashtags = Pattern.compile("\"hashtags\": \\[(.*?(?=\\],))\\],");
			Matcher m_hashtags = p_hashtags.matcher(value.toString());
			Pattern p_content = Pattern.compile(", \"text\": \"(.*?(?=\", \"truncated\"))");
			Matcher m_content;
			Pattern p_user_info = Pattern.compile("\"user\": \\{(.*?\"translator_type\": \".*?(?=\")\")");
			Matcher m_user_info;
			Pattern p_name = Pattern.compile("\"name\": \"(.*?(?=\",))\",");
			Matcher m_name;
			Pattern p_screen_name = Pattern.compile("\"screen_name\": \"(.*?(?=\", \"l))\",");
			Matcher m_screen_name;
			Pattern p_location = Pattern.compile("\"location\": \"(.*?(?=\", \"d))\",");
			Matcher m_location;
			while(m_tweetjsons.find()) {
				tweetjson = m_tweetjsons.group(1);
				m_content = p_content.matcher(tweetjson);
				while(m_content.find()) {
					if (content.length() == 0) content = m_content.group(1);
					else content += ", " + m_content.group(1);
				}
				m_hashtags = p_hashtags.matcher(tweetjson);
				while(m_hashtags.find()) {
					if (hashtags.length() == 0) hashtags = m_hashtags.group(1);
					else hashtags += ", " + m_hashtags.group(1);
				}
				m_user_info = p_user_info.matcher(tweetjson);
				while(m_user_info.find()) {
					user = m_user_info.group(1);

					m_name = p_name.matcher(user);
					while (m_name.find()) {
						if (name.length() == 0) name = m_name.group(1);
						else name += ", " + m_name.group(0);
					}
					m_screen_name = p_screen_name.matcher(user);
					while (m_screen_name.find()) {
						if (screenname.length() == 0) screenname = m_screen_name.group(1);
						else screenname += ", " + m_screen_name.group(1);
					}
					m_location = p_location.matcher(user);
					while (m_location.find()) {
						if (location.length() == 0) location = m_location.group(1);
						else location += m_location.group(1);
					}
				}
				// output value: [1, [name], [screenname], [hashtags], [content], [location]]
				tweet = "[1, ["+name+"], [" + screenname + "], [" + hashtags + "], [" + content + "], [" + location + "]]";
				Text tweety = new Text(tweet);
				for (String word:content) {
					context.write(new Text(word), tweety);
				}
			}
		}
	}


	// Input key:	Text Hashtag
	// Input value:	Text Tweet		OR StringArrayWritable TweetInfo (NEW)
	// Output key:	Text Hashtag
	// Ouput value: StringArrayWritable Tweets
/public static class TweetReducer extends Reducer<Text, Text, Text, ArrayWritable> {
//	public static class TweetReducer extends Reducer<Text, StringArrayWritable, Text, StringArrayWritable> {	// NEW2

		public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
//		public void reduce(Text key, Iterable<StringArrayWritable> values, Context context) throws IOException, InterruptedException{

	
			Pattern p_digit = Pattern.compile("\\[(\\d+)(.*)");
			Matcher m_digit;
			int num;
			String rest;
			for (Text val:values) {
				m_digit = p_digit.matcher(val.toString());
				while(m_digit.find()) {
					num = Integer.parseInt(m_digit.group(1));
				}
				++num;
				// TODO Oh crap. Have to add all tweet jsons to a list.....
			}
			String new_val = 			

		
//			context.write(key, new ArrayWritable(res));
			context.write(key, new StringArrayWritable(res));
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
	job.setOutputValueClass(StringArrayWritable.class);	// NEW

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
