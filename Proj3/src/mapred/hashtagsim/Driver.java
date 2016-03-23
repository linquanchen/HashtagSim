package mapred.hashtagsim;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.FileUtil;
import mapred.util.SimpleParser;
import mapred.util.InputLines;
import java.util.HashSet;
import mapred.filesystem.CommonFileOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static HashSet<String> tags = new HashSet<String>();

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String tmpdir = parser.get("tmpdir");

		// getJobFeatureVector(input, tmpdir + "/job_feature_vector");

		// String jobFeatureVector = loadJobFeatureVector(tmpdir
		// 		+ "/job_feature_vector");

		// System.out.println("Job feature vector: " + jobFeatureVector);

		getHashtagFeatureVector(input, tmpdir + "/feature_vector");
        convertHashtagFeatureVector(tmpdir + "/feature_vector", tmpdir + "/new_feature_vector");
		getAllHashtagSimilarities(tmpdir + "/new_feature_vector", output);

		// getHashtagSimilarities(jobFeatureVector, tmpdir + "/feature_vector",
		// 		output);
	}

	/**
	 * Computes the word cooccurrence counts for hashtag #job
	 * 
	 * @param input
	 *            The directory of input files. It can be local directory, such
	 *            as "data/", "/home/ubuntu/data/", or Amazon S3 directory, such
	 *            as "s3n://myawesomedata/"
	 * @param output
	 *            Same format as input
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getJobFeatureVector(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for hashtag #Job");

		job.setClasses(JobMapper.class, JobReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.setReduceJobs(1);

		job.run();
	}

	/**
	 * Loads the computed word cooccurrence count for hashtag #job from disk.
	 * 
	 * @param dir
	 * @return
	 * @throws IOException
	 */
	private static String loadJobFeatureVector(String dir) throws IOException {
		// Since there'll be only 1 reducer that process the key "#job", result
		// will be saved in the first result file, i.e., part-r-00000
		String job_featureVector = FileUtil.load(dir + "/part-r-00000");

		// The feature vector looks like "#job word1:count1;word2:count2;..."
		String featureVector = job_featureVector.split("\\s+", 2)[1];
		return featureVector;
	}

	/**
	 * Same as getJobFeatureVector, but this one actually computes feature
	 * vector for all hashtags.
	 * 
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	private static void getHashtagFeatureVector(String input, String output)
			throws Exception {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
				"Get feature vector for all hashtags");
		job.setClasses(HashtagMapper.class, HashtagReducer.class, null);
		job.setMapOutputClasses(Text.class, Text.class);
		job.run();
	}

	/**
	 * When we have feature vector for both #job and all other hashtags, we can
	 * use them to compute inner products. The problem is how to share the
	 * feature vector for #job with all the mappers. Here we're using the
	 * "Configuration" as the sharing mechanism, since the configuration object
	 * is dispatched to all mappers at the beginning and used to setup the
	 * mappers.
	 * 
	 * @param jobFeatureVector
	 * @param input
	 * @param output
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void getHashtagSimilarities(String jobFeatureVector,
			String input, String output) throws IOException,
			ClassNotFoundException, InterruptedException {
		// Share the feature vector of #job to all mappers.
		Configuration conf = new Configuration();
		conf.set("jobFeatureVector", jobFeatureVector);
		
		Optimizedjob job = new Optimizedjob(conf, input, output,
				"Get similarities between #job and all other hashtags");
		job.setClasses(SimilarityMapper.class, null, null);
		job.setMapOutputClasses(IntWritable.class, Text.class);
		job.run();
	}
    
    private static void convertHashtagFeatureVector(String input, String output) 
        throws IOException, ClassNotFoundException, InterruptedException {
        
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                "Convert the feature vector");
        job.setClasses(ConvertMapper.class, ConvertReducer.class, null);
        job.setMapOutputClasses(Text.class, Text.class);
        job.run();
    }

	private static void getAllHashtagSimilarities(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
    
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output, 
                "Get similarities between all hashtags");
        job.setClasses(SimilarityMapper.class, SimilarityReducer.class, null);
        job.setMapOutputClasses(Text.class, IntWritable.class);
        job.run();
        
    }	
//    private static void getAllHashtagSimilarities(String input, String output) 
//			throws IOException, ClassNotFoundException, InterruptedException {
//		String[] fileNames = CommonFileOperations.listAllFiles(input, ".*part.*", false);
//		System.out.println("there are " + fileNames.length + " files in total");
//
//		for (int i = 0; i < fileNames.length; i++) {
//			String tmpFileName = fileNames[i];
//			InputLines lines = FileUtil.loadLines(tmpFileName);
//			int count = 0;
//			for (String line : lines) {
//				String featureVector = line.split("\\s+", 2)[1];
//				String firstTag = line.split("\\s+", 2)[0];
//				String outputName = output + "/part" + count;
//
//				Configuration conf = new Configuration();
//				conf.set("featureVector", featureVector);
//				conf.set("firstTag", firstTag);
//				tags.add(firstTag);
//				
//				Optimizedjob job = new Optimizedjob(conf, input, outputName,
//						"Get similarities between " + firstTag + " and all other hashtags");
//				job.setClasses(SimilarityMapper.class, null, null);
//				job.setMapOutputClasses(IntWritable.class, Text.class);
//				job.run();
//				count++;
//			}
//		}
//
//	}

}


