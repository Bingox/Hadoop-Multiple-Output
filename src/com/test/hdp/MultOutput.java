package com.test.hdp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MultOutput {
	public static class DSMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String s = value.toString();
			String[] a = s.split("\t");
			context.write(new IntWritable(Integer.parseInt(a[1])), new Text(
					(a[0].substring(4).equals("2G") ? "2G" : a[0])));
		}
	}

	public static class MyOutputFormat extends
			MultipleOutputFormat<IntWritable, Text> {
		@Override
		protected String generateFileNameForKeyValue(IntWritable key,
				Text value, Configuration conf) {
			String t = value.toString();
			if (!t.isEmpty()) {
				return t;
			}
			return "other";
		}

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.105.34.213:8020");

		String[] ioArgs = new String[] { "/user/hdfs/ActiveNetworkSpeed",
				"/user/hdfs/ActiveDwSpeedSort" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "mo");

		job.setJarByClass(MultOutput.class);
		job.setMapperClass(DSMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(MyOutputFormat.class);

		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(ioArgs[1]));
		FileInputFormat.addInputPath(job, new Path(ioArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(ioArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}