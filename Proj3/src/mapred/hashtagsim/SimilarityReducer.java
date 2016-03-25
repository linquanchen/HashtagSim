package mapred.hashtagsim;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, Text, IntWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {		
        
        // Input: #a    #b:3;#c:2;#d:1
        //        #a    #c:1;#d:2;#e:2
        // Convert input to key-value pairs, 
        // Such as ("#b",3), ("#c",3), ("#d",3), ("#e", 2)        
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (Text hashtags : value) {
            String[] hashtag = hashtags.toString().split(";");
            for (int i = 0; i < hashtag.length; i++) {
                String[] str = hashtag[i].split(":");
                String tag = str[0];
                int count = Integer.parseInt(str[1]);
                if (map.containsKey(tag)) {
                    map.put(tag, map.get(tag) + count);
                }
                else {
                    map.put(tag, count);
                }
            }
        }
        
        for (Map.Entry<String, Integer> e : map.entrySet()) 
            context.write(new IntWritable(e.getValue()), 
                    new Text(key.toString() + ":" + e.getKey()));
	}
}
