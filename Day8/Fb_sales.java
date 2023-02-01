import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;

                  
public class Fb_sales

{
	// Mapper class -> output -> string, int
	public static class Fb_likesMapper extends MapReduceBase implements Mapper <Object, Text, IntWritable, IntWritable> 
	{	
		boolean flag=false;
		public void map(Object key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException
		{
			String line[] = value.toString().split(",",10);
			if(flag) 
			{
				String date = line[2];
				int shares = Integer.parseInt(line[5]);
				if(date.contains("2017"))
				{
					String x[] = date.split("/",3);
					int month=Integer.parseInt(x[0]);
					output.collect(new IntWritable(month), new IntWritable(shares));
				}	
			}
			flag=true;		
		}
	}
	// Reducer class -> string, int
	public static class Fb_likesReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, FloatWritable> 
	{
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector <IntWritable,FloatWritable> output,Reporter reporter) throws IOException
		{
			int add=0, total=0;
			while(values.hasNext()) {
				add=add+values.next().get();
				total++;
			}
		output.collect(key, new FloatWritable(add/(float)total));
		}
	}
	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(Fb_likes1.class);
	conf.setJobName("Internet Log");
	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(FloatWritable.class);
	conf.setMapperClass(Fb_likesMapper.class);
	conf.setReducerClass(Fb_likesReducer.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	JobClient.runJob(conf);
	}
}
