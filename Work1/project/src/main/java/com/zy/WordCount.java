package com.zy;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static class Map extends
			Mapper<LongWritable, Text, Text, FlowBean> {
		private final static FlowBean flow = new FlowBean();
		private final Text phone = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split("\t");
			flow.set(Long.parseLong(data[8]), Long.parseLong(data[9]));
			phone.set(data[1]);
			context.write(phone, flow);
		}
	}

	public static class Reduce extends
			Reducer<Text, FlowBean, Text, FlowBean> {

		public void reduce(Text key, Iterable<FlowBean> values,
				Context context) throws IOException, InterruptedException {
			long upFlow = 0;
			long downFlow = 0;
			for (FlowBean val : values) {
				upFlow += val.getUpFlow();
				downFlow += val.getDownFlow();
			}
			context.write(key, new FlowBean(upFlow, downFlow));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "zhangyang");
		job.setJarByClass(WordCount.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}