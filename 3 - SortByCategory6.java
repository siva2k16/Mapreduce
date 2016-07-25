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

public class SortByCategory6
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
            	if(tokens.length == 4)
            	{
	                tokens[i] = itr.nextToken();
	                if(i==0)
	                {
	                	category = tokens[0];
	                }
	                if(i==1)
	                {
	                	title = tokens[1]+"_T";
	                }                
	                if(i==2)
	                {
	                	productid = tokens[i] +"_P";
	                }
	                if(i==3)
	                {
	                	wrating = tokens[i] +"_R";
	                	if(productid !="-1" && title !="-1" && wrating !="-1")
	                	{
		 	                context.write(new Text(category), new Text(title + "|||" + productid +"|||"+ wrating ));
	                	}
	                }
            	}
            }
        }
    }
        
    //Category-key //Title_ProductId_Rating
    //Hashmap - Key - Title_ProductId - _Rating (value)
    //Reduce - Key - Title_ProductId - _Rating (value)
    
    public static class IntSumReducer extends Reducer<Text, Text, Text, Text>
    {
    	private Map<Text, FloatWritable> countMap = new HashMap<>();
    	private Map<Text, Text> ItemtMap = new HashMap<>();

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
            int sum = 0;
            countMap.put(new Text(key), new FloatWritable(sum));
        	String productid = "-1", tempval = "";
        	String category = "-1";
        	String ratingtemp="-1";
        	String title = "-1";
        	float rateval = 0;
        	String categoryvalue = key.toString(); 
        	List<String> catlist = new ArrayList<String>();
          	catlist.clear();
        	for(Text val: values)
    		{
        		//String split with commas
            	StringTokenizer itr = new StringTokenizer(val.toString(),"|||");
            	String tokens[] = new String[itr.countTokens()];
                for(int i=0; i<tokens.length; i++)
                {
                    tokens[i] = itr.nextToken();
                    tempval = tokens[i];
               		if(tempval.contains("_T"))
               		{
               			title = tempval;
               			title = title.replace("\'","");
               			title = title.replace("\t","");       
               			title = title.replace(" ","");       
               			//title = title.replace("_T","");
               			title = title.replace("&quot","");
               			title = title.trim();
               		}
               		if(tempval.contains("_P"))
               		{
                       	productid = tempval.toString();
                       	productid = productid.replace("\'","");
                       	productid = productid.replace("\t","");       
                       	productid = productid.replace(" ",""); 
                       	productid = productid.replace("&quot","");
                       	productid = productid.trim();
               		}
               		if(tempval.contains("_R"))
               		{
               			ratingtemp = tempval;
               			ratingtemp = ratingtemp.replace("\'","");
               			ratingtemp = ratingtemp.replace("\t","");       
               			ratingtemp = ratingtemp.replace(" ","");       
                		ratingtemp = ratingtemp.replace("_R","");
                		ratingtemp = ratingtemp.replace("&quot","");
                		ratingtemp = ratingtemp.trim();
                		if(isFloat(ratingtemp))
                		{
	                		rateval = Float.valueOf(ratingtemp);
	                   		if(title !="-1" && productid !="-1" && ratingtemp!="-1")
	                   		{
	                   			countMap.put(new Text(val), new FloatWritable(rateval));  
	                   		}
                		}
               		}
                }
            }

            Map<Text, FloatWritable> sortedMap = sortByValues(countMap);
            
            int counter = 0;
            for (Text keyval : sortedMap.keySet()) 
            {
                if (counter++ == 5) 
                {
                    break;
                }
                
            	StringTokenizer itr = new StringTokenizer(keyval.toString(),"|||");
            	String tokens[] = new String[itr.countTokens()];
                for(int i=0; i<tokens.length; i++)
                {
                    tokens[i] = itr.nextToken();
                    tempval = tokens[i];
               		if(tempval.contains("_T"))
               		{
               			title = tempval;
               			title = title.replace("\'","");
               			title = title.replace("\t","");       
               			title = title.replace(" ","");       
               			title = title.replace("_T","");
               			title = title.replace("&quot","");
               			title = title.trim();
               		}
               		if(tempval.contains("_P"))
               		{
                       	productid = tempval.toString();
                       	productid = productid.replace("\'","");
                       	productid = productid.replace("\t","");       
                       	productid = productid.replace("_P","");
                       	productid = productid.replace(" ",""); 
                       	productid = productid.replace("&quot","");
                       	productid = productid.trim();
               		}
               		if(tempval.contains("_R"))
               		{
               			ratingtemp = tempval;
               			ratingtemp = ratingtemp.replace("\'","");
               			ratingtemp = ratingtemp.replace("\t","");       
               			ratingtemp = ratingtemp.replace(" ","");       
                		ratingtemp = ratingtemp.replace("_R","");
                		ratingtemp = ratingtemp.replace("&quot","");
                		ratingtemp = ratingtemp.trim();
                		if(isFloat(ratingtemp))
                		{
	                		rateval = Float.valueOf(ratingtemp);
	                   		if(title !="-1" && productid !="-1" && ratingtemp!="-1")
	                   		{
	                   			context.write(new Text(categoryvalue), new Text(","+ productid + ","+ title + "," + ratingtemp ));
	                        	//context.write(new Text(categoryvalue), new Text(sortedMap.get(keyval) + ","+ keyval.toString()));
	                   		}
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
        job.setJarByClass(SortByCategory6.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/input3"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_sorted_category_exercise3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
