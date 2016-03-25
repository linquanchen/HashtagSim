package mapred.hashtagsim;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
        String[] featureVector = line.split("\\s+", 2); 
        String[] hashtags = featureVector[1].split(";");
        
        // Sort the String array based on the #hashtag.
        sort(hashtags);
        
        // Store the hashtag and count into maps
        int len = hashtags.length; 
        Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();
        Map<Integer, String> hashtagMap = new HashMap<Integer, String>();
        for (int i = 0; i < len; i++) {
            String[] str = hashtags[i].split(":");
            countMap.put(i, Integer.parseInt(str[1]));
            hashtagMap.put(i, str[0]);
        }
        
        // Compute Similarity
        for (int i = 0; i < len - 1; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = i + 1; j < len; j++) {
                sb.append(hashtagMap.get(j) + ":" + countMap.get(i) * countMap.get(j) + ";");
            }
            context.write(new Text(hashtagMap.get(i)), new Text(sb.toString()));
        }
	}
    
    /**
     * Sort the String array based on the #hashtag.
     *
     * @param features #hashtag:count
     * 
     */
    public void sort(String[] features) {
        List<String> list = Arrays.asList(features);
        Collections.sort(list, new Comparator<String>() {
            public int compare(String s1, String s2) {
                return s1.split(":")[0].compareTo(s2.split(":")[0]);
            }
        });
        features = list.toArray(features);
    }
}
