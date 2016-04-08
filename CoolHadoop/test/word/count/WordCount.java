package word.count;

/*import WordCount;*/

import fs.Path;
import io.IntWritable;
import io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

import coolmapreduce.Configuration;
import coolmapreduce.Context;
import coolmapreduce.Job;
import coolmapreduce.Mapper;
import coolmapreduce.Reducer;
import coolmapreduce.lib.input.FileInputFormat;
import coolmapreduce.lib.output.FileOutputFormat;

/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;*/

/*import WordCount.IntSumReducer;
import WordCount.TokenizerMapper;*/

public class WordCount {

	  public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, IntWritable>{
		  
		  @Override
		public void setup(Context context) {
			  System.out.println("insetup");
			super.setup(context);
		}

	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      StringTokenizer itr = new StringTokenizer(value.toString());
	      while (itr.hasMoreTokens()) {
	        word.set(itr.nextToken());
	        System.out.println("word is " + word + " with " + one);
//	        context.write(word, one);
	      }
	    }
	    
	    @Override
	    public void cleanup(Context context) {
	    	// TODO Auto-generated method stub
	    	System.out.println("incleanup");
	    	super.cleanup(context);
	    }
	  }
	  
	  

//	  public static class IntSumReducer
//	       extends Reducer<Text,IntWritable,Text,IntWritable> {
//	    private IntWritable result = new IntWritable();
//
//	    public void reduce(Text key, Iterable<IntWritable> values,
//	                       Context context
//	                       ) throws IOException, InterruptedException {
//	      int sum = 0;
//	      for (IntWritable val : values) {
//	        sum += val.get();
//	      }
//	      result.set(sum);
////	      context.write(key, result);
//	    }
//	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf);
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    
//	    job.setReducerClass(IntSumReducer.class);
//	    job.setOutputKeyClass(Text.class);
//	    job.setOutputValueClass(IntWritable.class);
//	    FileInputFormat.addInputPath(job, new Path(args[0]));
//	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}