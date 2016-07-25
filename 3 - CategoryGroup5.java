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

import com.amazonaws.services.support.model.Category;

public class CategoryGroup5
{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static FloatWritable one = new FloatWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
        	String rating ="-1";
        	String category ="-1";
        	String productid = "-1";
        	String title = "-1";
        	float wrating=0;
        	StringTokenizer itr = new StringTokenizer(value.toString(),"|||");
        	String tokens[] = new String[itr.countTokens()];
          	List<String> catlist = new ArrayList<String>();
          	catlist.clear();
            for(int i=0; i<tokens.length; i++)
            {
            	if(tokens.length==2)
            	{
                    tokens[i] = itr.nextToken();
                    if(i==0)
                    {
                    	productid = tokens[0];
                    }
                    if(i==1)
                    {
                  		rating = tokens[i] + "_R";
                  		//send review ratings
	                	context.write(new Text(productid), new Text(rating));
                    }
            	}
            	else if(tokens.length>2)
            	{
                    tokens[i] = itr.nextToken();
                    if(i==0)
                    {
                    	productid = tokens[0];
                    }
                    if(i==1)
                    {
                    	title = tokens[1];
                    	title = title+"_T";
                    }
                    if(i>=2)
                    {
                    	category = tokens[i];
       					catlist.add(category);
                    }
            	}
            }

            if(catlist.size() > 0 && title!="-1")
            {
	          	for(int i = 0; i < catlist.size(); i++)
	          	{
	          		String categoryname = catlist.get(i);
	          		categoryname = categoryname + "_C";
	          		//Send Category Values
	        		context.write(new Text(productid), new Text(categoryname));          			
	        		context.write(new Text(productid), new Text(title));
	          	}
            }
        }
    }
    
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    {
    	private FloatWritable result = new FloatWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
        	String productid = "-1", tempval = "";
        	String category = "-1";
        	String ratingtemp="-1";
        	String title = "-1";
          	List<String> catlist = new ArrayList<String>();
          	catlist.clear();
        	for(Text val: values)
    		{
        		tempval = val.toString();
               	productid = key.toString();
               	productid = productid.trim();

           		if(tempval.contains("_R"))
           		{
           			ratingtemp = tempval;
            		ratingtemp = ratingtemp.replace("_R","");
            		ratingtemp = ratingtemp.trim();
           		}
            	
           		if(tempval.contains("_T"))
           		{
           			title = tempval;
           			title = title.replace("_T","");
           			title = title.trim();
           		}
            			
           		
           		if(tempval.contains("_C"))
           		{
           			category = tempval;
           			category = category.replace("[[","");
           			category = category.replace("\t","");
           			category = category.replace("[[u\'","");
           			category = category.replace("]]","");
           			category = category.replace("u\'","");
           			category = category.replace("\'","");
           			category = category.replace("_C","");
           			category = category.trim();
           			catlist.add(category);
           		}
            }
            
        	if(catlist.size()>0 && ratingtemp!="-1" && productid !="-1")
        	{
        		String categoryname = "-1";
        		for(int i = 0; i < catlist.size(); i++)
	          	{
	          		categoryname = catlist.get(i);
	          		if(!ratingtemp.equals("-1") && categoryname!="-1")
	          		{
	        			context.write(new Text(categoryname + "|||"), new Text(title + "|||" + productid + "|||" + ratingtemp));          			
	          		}
	          	}
        	}
          	ratingtemp = "-1";
          	catlist.clear();
        }
   }
   public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(CategoryGroup5.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/input2"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_category_product_rating_group"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
