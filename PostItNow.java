
package org.apache.hadoop.examples;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author pradeep kumar m
 *
 */
public class PostItNow {

	/* driver code */
	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: MaxClosePrice <input path> <output path>");
			System.exit(-1);
		}

		// Define MapReduce job
		final Job job = new Job(conf, "postnow");
		job.setJarByClass(PostItNow.class);
		job.setJobName("postnow");

		// Set input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set Input and Output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set Mapper and Reduce classes
		job.setMapperClass(MaxClosePriceMapper.class);
		job.setReducerClass(MaxClosePriceReducer.class);

		// Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class MaxClosePriceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		static int search(String[] arr, String s) {
			int counter = 0;
			for (int j = 21; j < arr.length; j++)

				/*
				 * checking if string given in query is present in the given
				 * string. If present, increase times
				 */
				if (s.equals(arr[j].trim()))
					counter += 1;

			return counter;
		}

		static int answerQueries(String[] arr, String q) {
			return search(arr, q);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] items = line.split(","); // split the line into string
												// array

			String marketName = items[1]; // get the market name

			String q = "y";
			int closePrice = answerQueries(items, q); // returns the number of
														// yes in single line

			context.write(new Text(marketName), new IntWritable(closePrice)); // write
																				// it
																				// as
																				// mapper
																				// output

		}
	}

	public static class MaxClosePriceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int maxClosePrice = 0;

			// Iterate all yes values count for a line and calculate total
			for (IntWritable value : values) {
				maxClosePrice += value.get();
			}

			maxClosePrice = (maxClosePrice * 100 / 24); // calculate percentage
			// Write output

			if (maxClosePrice >= 60) {
				context.write(key, new IntWritable(maxClosePrice));
			}

		}
	}

}
