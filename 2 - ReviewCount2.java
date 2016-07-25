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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReviewCount2
{

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
 //       private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
        	StringTokenizer itr = new StringTokenizer(value.toString(),"|||");
        	String tokens[] = new String[itr.countTokens()];
            for(int i=0; i<tokens.length; i++){
                tokens[i] = itr.nextToken();
                if(i==9)
                	context.write(new Text(tokens[i]), one);
            }
        	 
        }
    }
    
    public static class TopNCombiner extends Reducer<Text,IntWritable, Text, IntWritable>
    {
    	@Override
    	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    	{
    		int sum=0;
    		for(IntWritable val: values)
    		{
    			sum+=val.get();
    		}
    		context.write(key,new IntWritable(sum));
    	}
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
    	private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // puts the number of occurrences of this word into the map.
            // We need to create another Text object because the Text instance
            // we receive is the same for all the words
            countMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                //if (counter++ == 500) {
                //    break;
                //}
                context.write(key, sortedMap.get(key));
            }
        }        
    }
    
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) 
    {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() 
        		{
		            @Override
		            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) 
		            {
		                return o2.getValue().compareTo(o1.getValue());
		            }
        		});
        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : entries) 
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

   public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ReviewCount2.class);
        
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(TopNCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/home/cloudera/Public/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/Public/output_review_dates"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
        
}
