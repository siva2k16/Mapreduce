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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortExpensiveItems9
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
        	String wrating="-1";
        	StringTokenizer itr = new StringTokenizer(value.toString(),"|||");
        	String tokens[] = new String[itr.countTokens()];
            for(int i=0; i<tokens.length; i++){
            	if(tokens.length == 3)
            	{
	                tokens[i] = itr.nextToken();
	                if(i==0)
	                {
	                	productid = tokens[i] + "_P";
	                }
	                if(i==1)
	                {
	                	title = tokens[i];
	                }                
	                if(i==2)
	                {
	                	category = tokens[i];
	                	if(!category.equals("-1"))
	                	{
	                		context.write(new Text(productid), new Text(title + "|||" + productid +"|||"+ category));
	                	}
	                }
            	}
            	if(tokens.length == 2)
            	{
	                tokens[i] = itr.nextToken();
	                if(i==0)
	                {
	                	productid = tokens[i]+"_P";
	                }
	                if(i==1)
	                {
	                	wrating = tokens[i];
	                	wrating= wrating + "_R";
	                	context.write(new Text(productid), new Text(wrating));
	                }                
            	}
            }
        }
    }
    
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    {
    	private Map<Text, FloatWritable> countMap = new HashMap<>();

    	public boolean isFloat(String string)
        {
        	try
        	{
        		Float.valueOf(string);
        		return true;
        	}
        	catch(NumberFormatException e)
        	{
        		return false;
        	}
        }
    	
    	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	String productid = "-1", tempval = "";
        	String category = "-1";
        	String ratingtemp="-1";
        	String title = "-1";
        	float rateval = 0;

        	for(Text val: values)
    		{
        		//String split with pipes
            	StringTokenizer itr = new StringTokenizer(val.toString(),"|||");
                for(int i=0; i<itr.countTokens(); i++)
                {
                	tempval = itr.nextToken();
                	if(tempval.contains("_T"))
               		{
               			title = tempval;
               			title = title.replace("\'","");
               			title = title.replace("\t","");       
               			title = title.replace("&quot","");
               			title = title.trim();
               		}
             		if(tempval.contains("_P"))
               		{
             			productid = tempval;
                      	//productid = key.toString();
                       	productid = productid.replace("\'","");
                       	productid = productid.replace("\t","");       
                       	productid = productid.replace("&quot","");
                       	productid = productid.trim();
               		}
               		if(tempval.contains("_C"))
               		{
               			category = tempval;
               			category = category.replace("\'","");
               			category = category.replace("\t","");       
               			category = category.replace("&quot","");
               			category = category.trim();
               		}
               		if(tempval.contains("_R"))
               		{
               			ratingtemp = tempval;
               			ratingtemp = ratingtemp.replace("\'","");
               			ratingtemp = ratingtemp.replace("\t","");       
                		ratingtemp = ratingtemp.replace("_R","");
                		ratingtemp = ratingtemp.replace("&quot","");
                		ratingtemp = ratingtemp.trim();
                		
                		if(isFloat(ratingtemp))
                		{
	                		rateval = Float.valueOf(ratingtemp);
	                		String keyvaldata = productid+"|||"+title+"|||"+category;
	                   		if(!title.equals("-1") && !productid.equals("-1") && !ratingtemp.equals("-1"))
	                   		{
	                   			countMap.put(new Text(keyvaldata), new FloatWritable(rateval));  
	                   		}
                		}
               		}
                }
            }
         }
        
        protected void cleanup(Context context) throws IOException, InterruptedException 
        {

        	String productid = "-1", tempval = "";
        	String category = "-1";
        	String ratingtemp="-1";
        	String title = "-1";
        	int counter = 0;
        	float rateval = 0;
        	
            Map<Text, FloatWritable> sortedMap = sortByValues(countMap);
            for (Text keyval : sortedMap.keySet()) 
            {
               	productid = "-1";
               	tempval = "";
            	category = "-1";
            	ratingtemp="-1";
            	title = "-1";
         
                if (counter++ == 10) 
                {
                    break;
                }
                
            	StringTokenizer itr = new StringTokenizer(keyval.toString(),"|||");
                for(int i=0; i<itr.countTokens(); i++)
                {
                	tempval = itr.nextToken();
               		if(tempval.contains("_T"))
               		{
               			title = tempval;
               			title = title.replace("\'","");
               			title = title.replace("\t","");       
               			title = title.replace("_T","");
               			title = title.replace("&quot","");
               			title = title.trim();
               		}
               		if(tempval.contains("_C"))
               		{
               			category = tempval;
               			category = category.replace("\'","");
               			category = category.replace("\t","");       
               			category = category.replace("_C","");
               			category = category.replace("&quot","");
               			category = category.trim();
               		}
               		if(tempval.contains("_P"))
               		{
               			productid = tempval;
               			productid = productid.replace("\'","");
               			productid = productid.replace("\t","");       
               			productid = productid.replace("_P","");
               			productid = productid.replace("&quot","");
               			productid = productid.trim();
               		}
               		
               		//context.write(new Text(productid), new Text(","+ title + ","+ category + "," + sortedMap.get(keyval)));
               		//context.write(new Text(keyval), new Text(","+ title + ","+ category + "," + sortedMap.get(keyval)));
               		if(!title.equals("-1") && !productid.equals("-1"))
                	{
                    	if(isFloat(ratingtemp))	                
	                	{
	                		context.write(new Text(productid), new Text(","+ title + "," + sortedMap.get(keyval)));
	                      	//context.write(new Text(categoryvalue), new Text(sortedMap.get(keyval) + ","+ keyval.toString()));
                  		}
               		}
                }
            }
        }        
    }
    
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) 
    {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

   public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SortExpensiveItems9.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/Input5"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_expensiveitems"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
