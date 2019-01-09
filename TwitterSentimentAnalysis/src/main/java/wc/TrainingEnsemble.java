
package wc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
 * The Class TrainingEnsemble.
 */
public class TrainingEnsemble extends Configured implements Tool {

	/**
	 * The Class Map.
	 */
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

		/** The files. */
		private URI[] files;

		/** The word map. */
		private HashMap<String, String> word_Map = new HashMap<String, String>();

		/** The bootstrap list. */
		List<String> bootstrapList = new ArrayList<>(0);

		/** The bootstrap final list. */
		List<String> bootstrapFinalList = new ArrayList<>();

		/** The cvs split by. */
		String cvsSplitBy = "\\|";

		/** The count. */
		int count = 0;

		/** The line. */
		String line = "";

		/** The rand. */
		int rand = 0;

		/** The nol. */
		int nol = 1;

//		@Override
//		public void setup(Context context) throws IOException {
//
//			try
//			{
//			String line = "";
//
//            URI[] cacheFiles = context.getCacheFiles();
//            for(int i=0; i<cacheFiles.length; i++)
//            {
//
//			 URI cacheFile = cacheFiles[i];
//
//        	 FileSystem fs = FileSystem.get(cacheFile, new Configuration());
//        	 InputStreamReader inputStream = new InputStreamReader(fs.open(new Path(cacheFile.getPath())));
//                BufferedReader reader = new BufferedReader(inputStream);
//                try
//                {
//
//                    while ((line = reader.readLine()) != null)
//                    {
//
//
//                    }
//                }
//
//                finally
//                {
//                	reader.close();
//                }
//            }
//        }
//			catch (IOException e)
//			{
//
//			}
//
//	}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
		 * org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			bootstrapList.add(value.toString());

			count++;

			for (String s : bootstrapList) {
				rand = 1 + (int) (Math.random() * ((10 - 1) + 1));
				if (rand % 4 == 0) {
					while (rand > 0) {
						bootstrapFinalList.add(s);
						rand = rand - 2;
					}
				}
			}

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper
		 * .Context)
		 */
		@Override
		// The cleanup() method is run once after map() has run for every row
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Collections.shuffle(bootstrapFinalList);

			for (int i = 0; i < bootstrapList.size(); i++) {
				context.write(NullWritable.get(), new Text(bootstrapFinalList.get(i)));
			}

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
//		if (args.length != 3) {
//			System.err.println("Usage: Parse <in> <out>");
//			System.exit(2);
//		}
		Job job = Job.getInstance(conf, "Twitter Sentiment Analysis");

		final Configuration jobConf = job.getConfiguration();

		jobConf.set("mapreduce.output.textoutputformat.separator", ";");

		Path path = new Path(args[2]);

//        FileSystem fs = FileSystem.get(new URI(args[2]), new Configuration());
//        FileStatus[] fileStat = fs.listStatus(path);
//
//        for(FileStatus f : fileStat)
//        {
//            job.addCacheFile(f.getPath().toUri());
//        }

		job.setJarByClass(TrainingEnsemble.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

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
		ToolRunner.run(new TrainingEnsemble(), args);
	}

}