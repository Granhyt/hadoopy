
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

public class Question2_2 {	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndIntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split("\\t", -1);
			Country temp = Country.getCountryAt(Double.parseDouble(fields[11]), Double.parseDouble(fields[10]));
			if (temp != null) {
				Text country = new Text(temp.toString());
				for (String tag : java.net.URLDecoder.decode(fields[8].toString()).split(",")) {
					StringAndIntWritable saiw = new StringAndIntWritable();
					saiw.set(new Text(tag), new IntWritable(1));
					context.write(new Text(country), saiw);
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, StringAndIntWritable, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<StringAndIntWritable> tags, Context context)
				throws IOException, InterruptedException {
			HashMap<String, IntWritable> hmap = new HashMap<String, IntWritable>();
			for (StringAndIntWritable e : tags) {
				if (hmap.containsKey(e.tag.toString()))
					hmap.put(e.tag.toString(), new IntWritable(hmap.get(e.tag.toString()).get() + 1));
				else
					hmap.put(e.tag.toString(), new IntWritable(e.number.get()));
			}
			int k = context.getConfiguration().getInt("k", 20);
			PriorityQueue<StringAndIntWritable> queue = new PriorityQueue<StringAndIntWritable>(k);
			for (Entry<String, IntWritable> e : hmap.entrySet()) {
				StringAndIntWritable saiw = new StringAndIntWritable();
				saiw.set(new Text(e.getKey()), e.getValue());
				queue.add(saiw);
			}
			String s = "";
			for (int i=0; i<k; i++) {
				StringAndIntWritable e = queue.poll();
				if (e != null) s += e.tag.toString() + ":" + e.number.toString() + " ";
			}
			context.write(key, new Text(s));
		}
	}
	
	public static class MyCombiner extends Reducer<Text, StringAndIntWritable, Text, StringAndIntWritable> {
		@Override
		protected void reduce(Text key, Iterable<StringAndIntWritable> tags, Context context)
				throws IOException, InterruptedException {
			HashMap<String, IntWritable> hmap = new HashMap<String, IntWritable>();
			for (StringAndIntWritable e : tags) {
				if (hmap.containsKey(e.tag.toString()))
					hmap.put(e.tag.toString(), new IntWritable(hmap.get(e.tag.toString()).get() + 1));
				else
					hmap.put(e.tag.toString(), e.number);
			}
			int k = context.getConfiguration().getInt("k", 20);
			PriorityQueue<StringAndIntWritable> queue = new PriorityQueue<StringAndIntWritable>(k);
			for (Entry<String, IntWritable> e : hmap.entrySet()) {
				StringAndIntWritable saiw = new StringAndIntWritable();
				saiw.set(new Text(e.getKey()), e.getValue());
				queue.add(saiw);
			}
			for (int i=0; i<k; i++) {
				StringAndIntWritable e = queue.poll();
				if (e != null) {
					context.write(key, e);
				}
			}
		}
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		int k = Integer.parseInt(otherArgs[2]);

		Job job = Job.getInstance(conf, "Question2_2");
		job.setJarByClass(Question2_2.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndIntWritable.class);
		
		job.setCombinerClass(MyCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringAndIntWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.getConfiguration().setInt("k", k);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
