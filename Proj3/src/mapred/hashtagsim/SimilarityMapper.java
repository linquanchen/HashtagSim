package mapred.hashtagsim;

import java.io.IOException;

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
        
        // Output: a    #b:2    
        context.write(new Text(featureVector[0]), new Text(featureVector[1]));
        
//        String hashtag = featureVector[0];
//        String[] features = featureVector[1].split(";");
//        /**
//         * Convert the #a b:1;c:2 to b #a:1 and c #a:2
//         */
//        for (String feature : features) {
//            String[] word_count = feature.split(":");
//            context.write(new Text(word_count[0]), new Text(hashtag + ":" + word_count[1]));
//        }
	}
}
