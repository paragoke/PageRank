package pageIndexing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CountPages {

	public static void main(String arg[]) throws IOException{
		
		Configuration conf = new Configuration();
		Job j = new Job(conf);
		j.setJobName("CountPages");
		
		
		j.setMapperClass(CountPagesMap.class);
		j.setCombinerClass(CountPagesReduce.class);
		j.setReducerClass(CountPagesReduce.class);
		
		
		j.setMapOutputKeyClass(String.class); // title of the xml file
		j.setMapOutputValueClass(IntWritable.class);// list of links i.e. titles
		
		j.setOutputKeyClass(String.class); // list of titles
		j.setOutputValueClass(IntWritable.class); // their corresponding links
		
		
		j.setInputFormatClass(TextInputFormat.class); //WikiDataInputFormat.class
		j.setOutputFormatClass(TextOutputFormat.class);
	}
	
	public class CountPagesMap extends Mapper <LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context c){
			// ongetting a key value pair
			// output title<key> one<value>
			
		}
		
	}
	
	public class CountPagesReduce extends Reducer <LongWritable, Text, Text, IntWritable>{
		
		// int count
		
		void reduce(LongWritable key, Text value, Context c){
			//increment count for evry key/value pair
		}
	}

}
