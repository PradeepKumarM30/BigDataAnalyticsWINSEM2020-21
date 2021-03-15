package org.apache.hadoop.examples;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PostItNow1 {

	/* driver code */
	public static void main(final String[] args) throws Exception {
		final Configuration conf = new Configuration();

		if (args.length != 2) {
			System.err.println("Usage: MaxClosePrice <input path> <output path>");
			System.exit(-1);
		}

		// Define MapReduce job
		final Job job = new Job(conf, "postnow1");
		job.setJarByClass(PostItNow.class);
		job.setJobName("postnow1");

		// Set input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Set Input and Output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set Mapper and Reduce classes
		job.setMapperClass(PairsMapper.class);
		job.setReducerClass(PairsReducer.class);

		// Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class PairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] items = line.split(",");

			Map<String, Integer> countOfPairs = new HashMap<String, Integer>();

			countOfPairs = new HashMap<String, Integer>(); // traverse evry pair
															// and add to list
			for (int count = 0; count < items.length - 1; count++) {
				if (countOfPairs.containsKey(items[count] + items[count + 1])) { // if
																					// not
																					// already
																					// present
					countOfPairs.put(items[count] + items[count + 1],
							countOfPairs.get(items[count] + items[count + 1]) + 1);
				} else {
					countOfPairs.put(items[count] + items[count + 1], 1); // if
																			// already
																			// present
				}
			}
			Set set = countOfPairs.entrySet();
			Iterator itr = set.iterator();
			while (itr.hasNext()) {
				// Converting to Map.Entry so that we can get key and value
				// separately
				Map.Entry entry = (Map.Entry) itr.next();
				context.write(new Text((String) entry.getKey()), new IntWritable((int) entry.getValue()));
			}

		}
	}

	public static class PairsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int pairsCount = 0;

			for (IntWritable value : values) { // Add all the values for a
												// particular pair
				pairsCount += value.get();
			}

			context.write(key, new IntWritable(pairsCount)); // write it to a
																// reducer
																// output
		}

	}
}
