package pageIndexing;

import java.io.IOException;
import java.util.ListIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

public class ExtractLinks {
	public static void main(String args[]) throws IOException{
		Configuration conf = new Configuration();
		Job j = new Job(conf);
		j.setJobName("ExtractLinks");
		
		
		j.setMapperClass(ExtractLinksMap.class);
		j.setCombinerClass(ExtractLinksReduce.class);
		j.setReducerClass(ExtractLinksReduce.class);
		
		
		j.setMapOutputKeyClass(String.class); // title of the xml file
		j.setMapOutputValueClass(#TODO);// list of links i.e. titles
		
		j.setOutputKeyClass(#TODO); // list of titles
		j.setOutputValueClass(#TODO); // their corresponding links
		
		
		j.setInputFormatClass(TextInputFormat.class); //WikiDataInputFormat.class
		j.setOutputFormatClass(TextOutputFormat.class);
		
		
	}
	
	class ExtractLinksMap extends Mapper <LongWritable, Text, Text, IntWritable>{
		
		
		public void map(LongWritable key, Text value, Context c){
			
			// get the xmlfile
		
			// find the title
			// replace space with an underscore
		
			// extract wikilinks using regex ([[a]] or [[a|b]] extract a out)
			// replace space with underscore here as well
		
			// write to file
		}
	}
	
	class ExtractLinksReduce extends Reducer <Text, IntWritable, Text, IntWritable>{
		
		// hashmap ds that holds all page titles<key> and links<value>
		
		void reduce(String key, ListIterator<String> li, Context c){
			
			// append the title and its wikilinks list to 
		}
		
		// write to context
	}
}


class WikiDataInputFormat extends TextInputFormat {

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit ginput, org.apache.hadoop.mapreduce.TaskAttemptContext context) { // requires importing related to logging

		return new WikiDataRecordReader();
	}
}


class WikiDataRecordReader extends RecordReader<LongWritable, Text> {

	
	private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    
    private final int NLINESTOPROCESS = 3;
    
    private long start =0;
    private long end =0;
    private long pos =0;
    
    private int maxLineLength;

    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
 
    @Override
    public LongWritable getCurrentKey() throws IOException,InterruptedException {
        return key;
    }
 
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
 
	@Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }
	
	@Override
	public void initialize(InputSplit genericSplit, org.apache.hadoop.mapreduce.TaskAttemptContext context)throws IOException, InterruptedException {
        
		FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        
        start = split.getStart();
        end= start + split.getLength();
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());
 
        if (start != 0){
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        in = new LineReader(filein,conf);
        if(skipFirstLine){
            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }
 
	@Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;
        for(int i=0;i<NLINESTOPROCESS;i++){
            Text v = new Text();
            while (pos < end) {
                newSize = in.readLine(v, maxLineLength,Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),maxLineLength));
                value.append(v.getBytes(),0, v.getLength());
                value.append(endline.getBytes(),0, endline.getLength());
                if (newSize == 0) {
                    break;
                }
                pos += newSize;
                if (newSize < maxLineLength) {
                    break;
                }
            }
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

}