
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
			//configuration用来传递，不想在mapper或reducer里面把任何一个parameter写死，也不会一步一步被读入或改变
			//如果n-Gram输入是null或者0，用default value n-gram = 5
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			line = line.trim().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");
			//change all non-alphabetical symbols to "space"
			//split into I, love, big, data   先拆分成一个个小单词，然后再做2-gram,...N-Gram的粘连
			
			String[] words = line.split("\\s+"); //split by ' ', '\t'...ect
			
			if(words.length<2) {
				return;
			}
			
			//I love big data
			StringBuilder sb;
			for(int i = 0; i < words.length-1; i++) {
				sb = new StringBuilder();
				sb.append(words[i]);
				for(int j=1; i+j<words.length && j<noGram; j++) {  //j = 需要粘连的单词的个数
					sb.append(" ");
					sb.append(words[i+j]);
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
					// 这个单词出现1次
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values) {
				sum += value.get();     //当前单词出现了几次
			}
			context.write(key, new IntWritable(sum));
		}
	}

}