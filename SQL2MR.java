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
import java.io.IOException;
import java.util.Map;
import java.io.IOException;
import java.util.*;
public class SQL2MR{
	public static class TokenizeMapper
	extends Mapper<Object, Text, Text, FloatWritable>{
		private Text word=new Text();
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
			String[] tokens = line.toString().split("\t");
			List<String> token=new ArrayList<String>();
            token=Arrays.asList(tokens);
			if (token.size()==4){
				int length= Integer.parseInt(tokens[2]);
				if (length>=60){
					FloatWritable value = new FloatWritable(Float.parseFloat(tokens[1]));
                    word.set(tokens[3]);
                	context.write(word, value);
				}
			}
		}
	}
	public static class CountReducer 
    extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		private FloatWritable rental_mean=new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			int sum=0;
			float rental_sum=0;
            for (FloatWritable val: values){
				sum += 1;
				rental_sum += val.get();
			}
			if (sum>=160){
				rental_mean.set(rental_sum / sum);
				context.write(key, rental_mean);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SQL2MR");
		job.setJarByClass(SQL2MR.class);
		job.setMapperClass(TokenizeMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
