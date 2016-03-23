package mapred.hashtagsim;

import java.io.IOException;
import java.util.*;
//import java.util.Map;
//import java.util.List;
//import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ConvertReducer extends Reducer<Text, Text, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<Text> value,
			Context context)
			throws IOException, InterruptedException {		

        List<String> list = new ArrayList<String>();
        for (Text hashtag : value) {
            list.add(hashtag.toString());
        }

        Collections.sort(list, new Comparator<String>() {
            public int compare(String s1, String s2) {
                return s1.split(":")[0].compareTo(s2.split(":")[0]);
            }
        });
        
        int len = list.size();
        String[] features = new String[len];
        features = list.toArray(features);
        for (int i = 0; i < len - 1; i++) {
            String[] firstTag = features[i].split(":");
            for (int j = i + 1; j < len; j++) {
                String[] secondTag = features[j].split(":");
                context.write(new Text(firstTag[0] + ":" + secondTag[0]),
                        new IntWritable(Integer.parseInt(firstTag[1]) 
                            * Integer.parseInt(secondTag[1])));
            }
        }
//        StringBuilder sb = new StringBuilder();
//
//        for (Text hashtag : value) {
//            sb.append(hashtag.toString() + ";");
//        }
//        sb.deleteCharAt(sb.length() - 1);
//        
//        context.write(key, new Text(sb.toString()));

//		Map<String, Integer> counts = new HashMap<String, Integer>();
//		for (Text word : value) {
//			String w = word.toString();
//			Integer count = counts.get(w);
//			if (count == null)
//				count = 0;
//			count++;
//			counts.put(w, count);
//		}
//		
//		StringBuilder builder = new StringBuilder();
//		for (Map.Entry<String, Integer> e : counts.entrySet()) 
//			builder.append(e.getKey() + ":" + e.getValue() + ";");
//		
//		context.write(key, new Text(builder.toString()));
	}
}
