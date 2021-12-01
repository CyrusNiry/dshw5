/*
 * @Author: Cyrus Yang
 * @Date: 2021-12-02 03:06:04
 * @LastEditTime: 2021-12-02 05:28:28
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Map;
import java.io.IOException;
import java.util.*;

public class SQL2MR{
	public static class CountMapper
	extends Mapper<Object, Text, Text, FloatWritable>{
		public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
			String[] cols = row.toString().split("\t");
			if (cols.length==4){
				int length = Integer.parseInt(cols[2]);
				if (length >= 60){
					context.write(new Text(cols[3]), new FloatWritable(Float.parseFloat(cols[1])));
				}
            }
		}
    }
    public static class CountReducer 
    extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		private FloatWritable rate_mean = new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> rates, Context context) throws IOException, InterruptedException {
			int count = 0;
			float rate_sum = 0;
            for (FloatWritable rate: rates){
				count += 1;
				rate_sum += rate.get();
			}
			if (count >= 160){
				rate_mean.set(rate_sum / count);
				context.write(key, rate_mean);
			}
		}
    }
    public static void main(String[] argv) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SQL2MR");
		job.setJarByClass(SQL2MR.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job,new Path(argv[0]));
		FileOutputFormat.setOutputPath(job,new Path(argv[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}