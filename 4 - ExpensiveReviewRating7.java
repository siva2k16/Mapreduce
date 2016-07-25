import java.io.IOException;
import java.util.ArrayList;
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

public class ExpensiveReviewRating7
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
        	
    	  	List<String> catlist = new ArrayList<String>();
    	  	catlist.add("expensive");
    	  	catlist.add("costly");
    	  	catlist.add("high-priced");
    	  	catlist.add("pricey");
    	  	catlist.add("overpriced");
    	  	catlist.add("premium");
    	  	String reviewcomments = "";
       	
        	StringTokenizer itr = new StringTokenizer(value.toString(),"|||");
        	String tokens[] = new String[itr.countTokens()];
            for(int i=0; i<tokens.length; i++)
            {
                tokens[i] = itr.nextToken();
                if(i==1)
                	{
                		productid = tokens[1] + "|||";
                	}
                if(i==5)
                	{
                		reviewcomments = tokens[5];
                	  	for(int j = 0; j < catlist.size(); j++)
                	  	{
                	  		if(reviewcomments.toLowerCase().contains(catlist.get(j).toLowerCase()))
                	  		{
                        		context.write(new Text(productid), one);                	  		
                        	}
                	  	}
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
               for (FloatWritable val : values)
               {
                   sum += val.get();
               }
               if(sum>0)
               {
            	   result.set(sum);
            	   context.write(key, result);
               }
        }
   }
   public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vote count");
        job.setJarByClass(ExpensiveReviewRating7.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        //reviews.csv file with pipe delimiters
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_review"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
