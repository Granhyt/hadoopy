
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2_1 {	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\t", -1);
			Country temp = Country.getCountryAt(Double.parseDouble(fields[11]), Double.parseDouble(fields[10]));
			if (temp != null) {
				Text country = new Text(temp.toString());
				for (String tag : java.net.URLDecoder.decode(fields[8].toString()).split("\\t+")) {				
					context.write(new Text(country), new Text(tag));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> tags, Context context)
				throws IOException, InterruptedException {
			HashMap<String,Integer> hmap = new HashMap<String, Integer>();
			for (Text tag : tags) {
				if (hmap.containsKey(tag.toString()))
					hmap.put(tag.toString(), hmap.get(tag.toString()) + 1);
				else
					hmap.put(tag.toString(), 1);
			}
			int k = context.getConfiguration().getInt("k", 20);
			PriorityQueue<StringAndInt> queue = new PriorityQueue<StringAndInt>(k);
			for (Entry<String, Integer> e : hmap.entrySet()) {
				queue.add(new StringAndInt(e.getKey(), e.getValue()));
			}
			for (StringAndInt e : queue) {
				context.write(new Text(e.tag), new Text(String.valueOf(e.number)));
			}
		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		int k = Integer.parseInt(otherArgs[2]);

		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.getConfiguration().setInt("k", k);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
