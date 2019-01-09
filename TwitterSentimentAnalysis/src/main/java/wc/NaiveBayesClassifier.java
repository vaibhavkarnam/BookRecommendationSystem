package wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Class NaiveBayesClassifier.
 */
@SuppressWarnings("deprecation")
public class NaiveBayesClassifier extends Configured implements Tool {

	/**
	 * The Class Map.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		/** The files. */
		private URI[] files;

		/** The word map. */
		private HashMap<String, String> word_Map = new HashMap<String, String>();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.
		 * Context)
		 */
		@SuppressWarnings("deprecation")
		@Override
		public void setup(Context context) throws IOException {

			try {
				String line = "";

				URI[] cacheFiles = context.getCacheFiles();
				for (int i = 0; i < cacheFiles.length; i++) {

					URI cacheFile = cacheFiles[i];

					FileSystem fs = FileSystem.get(cacheFile, new Configuration());
					InputStreamReader inputStream = new InputStreamReader(fs.open(new Path(cacheFile.getPath())));
					BufferedReader reader = new BufferedReader(inputStream);
					try {

						while ((line = reader.readLine()) != null) {
							String splits[] = line.split("\t");
							word_Map.put(splits[0], splits[1]);
						}
					}

					finally {
						reader.close();
					}
				}
			} catch (IOException e) {

			}

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String name;
			String twt;
			String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

			try {
				for (int i = 0; i < line.length; i++) {
					String tweet_id = line[0];
					String tweet_text = line[2];
					int sentiment_sum = 0;
					for (String word : tweet_text.split(" ")) {
						if (word_Map.containsKey(word)) {
							Integer x = new Integer(word_Map.get(word));
							sentiment_sum += x;
						}
					}

					context.write(new Text(tweet_id),
							new Text(tweet_text + "\t->\t" + new Text(Integer.toString(sentiment_sum))));

				}
			} catch (Exception e) {

			}
		}

	}

	/**
	 * The Class Reduce.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/** The word. */
		private final Text word = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable,
		 * org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			for (Text value : values) {
				word.set(value);
			}
			context.write(key, word);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2) {
			System.err.println("Usage: Parse <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Twitter Sentiment Analysis");

		final Configuration jobConf = job.getConfiguration();

		jobConf.set("mapreduce.output.textoutputformat.separator", ";");

		Path path = new Path(args[2]);

		FileSystem fs = FileSystem.get(new URI(args[2]), new Configuration());
		FileStatus[] fileStat = fs.listStatus(path);

		for (FileStatus f : fileStat) {
			job.addCacheFile(f.getPath().toUri());
		}

		job.setJarByClass(NaiveBayesClassifier.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new NaiveBayesClassifier(), args);
	}

}
