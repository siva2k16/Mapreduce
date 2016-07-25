import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

public class ProductTitleCategory8
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
    {
        private final static Text one = new Text();
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
        	String category = "-1";
        	String productid = "-1";
        	String title="-1";
        	float wrating=0;
        	StringTokenizer itr = new StringTokenizer(value.toString(),"|||");
        	StringTokenizer catitr;
        	List<String> catlist = new ArrayList<String>();
        	
        	String tokens[] = new String[itr.countTokens()];
            for(int i=0; i<tokens.length; i++){
                tokens[i] = itr.nextToken();
                if(i==0)
                {
                	productid = tokens[0];
                	productid = productid.replace("u'","");
                	productid = productid.replace("[[","");       
                	productid = productid.replace("]]","");
                	productid = productid.replace("\'","");
                	productid = productid.trim();
                	productid = productid+"|||";
                }
                if(i==1)
                {
                	title = tokens[1];
                	title = title.replace("u'","");
                	title = title.replace("[[","");       
                	title = title.replace("]]","");
                	title = title.replace("\'","");
                	title = title.trim();
                	title = title + "_T|||";
                }                
                if(i==6)
                {
                	if(title!="-1" && productid !="-1")
                	{
	                	category = "";
	                	category = tokens[i];
	                	catlist= Arrays.asList(category.split(","));
	                  	for(int p = 0; p < catlist.size(); p++)
	    	          	{
	    	          		String categoryname = catlist.get(p);
	    	          		category = categoryname.replace("u\'","");
	    	          		category = category.replace("u'","");
	    	          		category = category.replace("'","");
	                		category = category.replace("[[","");       
	                		category = category.replace("]]","");
	                		category = category.replace("\'",""); 
	    	          		categoryname = category + "_C";
	    	          		//Send Category Values
	     	                context.write(new Text(productid), new Text(title + categoryname));    
	     	                categoryname = "";
	     	            }
                	}
 	                //context.write(new Text(productid), new Text(title + tokens[i]));
                }
            }
        }
    }
    
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    {
    	@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		String category="";
    		String tempval = "";
    		boolean start=false;
    		
          	List<String> catlist = new ArrayList<String>();

    		for(Text val: values)
    		{
    			catlist.add(String(val));
    		}
    		 
          	for(int i = 0; i < catlist.size(); i++)
          	{
          		String categoryname = catlist.get(i);
          		if(categoryname.length()>1)
          		{
          			context.write(key, new Text(categoryname));
          		}
          	}
        }

		private String String(Text val) {
			return val.toString();
		}
   }
    
   public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ProductTitleCategory8.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		//metadata pipe seperated csv
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/input1"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_productcategory1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
