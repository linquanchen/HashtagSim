package mapred.hashtagsim;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeSimilarityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {

        String line = value.toString();
        String[] featureVector = line.split("\\s+", 2);
        
        // Output: #a:#b    2
        context.write(new Text(featureVector[0]), 
                new IntWritable(Integer.parseInt(featureVector[1])));

//		String line = value.toString();
//        String[] featureVector = line.split("\\s+", 2);
//		
//        String hashtag = featureVector[1];
//        String[] features = featureVector[1].split(";");
//        
//        sort(features);
//        
//        int len = features.length; 
//        for (int i = 0; i < len - 1; i++) {
//            String[] firstTag = features[i].split(":");
//            for (int j = i + 1; j < len; j++) {
//                String[] secondTag = features[j].split(":");
//                context.write(new Text(firstTag[0] + ":" + secondTag[0]),
//                        new IntWritable(Integer.parseInt(firstTag[1]) 
//                            * Integer.parseInt(secondTag[1])));
//            }
//        }
	}

//    public void sort(String[] features) {
//        List<String> list = Arrays.asList(features);
//        Collections.sort(list, new Comparator<String>() {
//            public int compare(String s1, String s2) {
//                return s1.split(":")[0].compareTo(s2.split(":")[0]);
//            }
//        });
//        features = list.toArray(features);
//    }
}
