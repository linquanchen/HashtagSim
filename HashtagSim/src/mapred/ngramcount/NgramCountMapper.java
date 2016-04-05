package mapred.ngramcount;

import java.io.IOException;

import mapred.util.Tokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Get the n from configuration
        Configuration conf = context.getConfiguration();
        final int n = Integer.parseInt(conf.get("N"));

        String line = value.toString();
		String[] words = Tokenizer.tokenize(line);

        int wordsLen = words.length;
        for (int i = 0; i < wordsLen - n + 1; i++) {
            // Get every n-grams
            StringBuilder sb = new StringBuilder();
            sb.append(words[i]);
            for (int j = i+1; j < i + n; j++) {
                sb.append(" " + words[j]);
            }
            context.write(new Text(sb.toString()), NullWritable.get());
        }
	}
}
