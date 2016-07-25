import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VoteCount3
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>
    {
        private final static FloatWritable one = new FloatWritable(1);
        private Text word = new Text();

        public boolean isInteger(String string)
        {
        	try
        	{
        		Integer.valueOf(string);
        		return true;
        	}
        	catch(NumberFormatException e)
        	{
        		return false;
        	}
        }
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
        	float upvotes=0, downvotes=0, totalvotes=0, rating=0;
        	String productid = "";
        	float wrating=0;
        	StringTokenizer itr = new StringTokenizer(value.toString(),"|||");
        	String tokens[] = new String[itr.countTokens()];
            for(int i=0; i<tokens.length; i++)
            {
                tokens[i] = itr.nextToken();
                if(i==1)
                	{
                		productid = tokens[1] + "|||";
                	}
                if(i==3)
                	if(isInteger(tokens[i]))
                	{
                		upvotes = Integer.parseInt(tokens[i]);
                	}
                if(i==4)
                	if(isInteger(tokens[i]))
                	{
                		totalvotes = Integer.parseInt(tokens[i]);
                	}
                if(i==6)
                	if(isInteger(tokens[i]))
                	{
                		rating = Integer.parseInt(tokens[i]);                
		                downvotes = totalvotes-upvotes;
		                wrating = ((upvotes-downvotes)/totalvotes)*rating;
		                context.write(new Text(productid), new FloatWritable(wrating));
                	}
            }
        	 
        }
    }
    
    public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
    {
    	private FloatWritable result = new FloatWritable();
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException 
        {
        	   float sum = 0;
        	   float count = 0;
               for (FloatWritable val : values)
               {
                   sum += val.get();
                   count=count+1;
               }
               if(sum>0 && count > 0)
               {
            	   sum = sum/count;
            	   result.set(sum);
            	   context.write(key, result);
               }
        }
   }
   public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vote count");
        job.setJarByClass(VoteCount3.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(1);
        
        //reviews.csv file with pipe delimiters
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_rating"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
