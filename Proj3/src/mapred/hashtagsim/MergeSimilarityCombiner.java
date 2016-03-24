package mapred.hashtagsim;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MergeSimilarityCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {		
	    
        int counts = 0;
        // Add all similarity number of one hashtag pair
        for (IntWritable count : value) {
            counts += count.get();
        }
		context.write(key, new IntWritable(counts));
	}
}
