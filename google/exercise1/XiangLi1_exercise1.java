package com.hadoop.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import java.io.FileInputStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class XiangLi1_exercise1 {


	public static class Pair implements Writable {

	// An implementation of value with tag, as a writable object

		private int count;
		private int sum;

		public Pair( ) {
			int count = 0;
			int sum = 0;
		}

		public Pair( int count, int sum ) {
			this.count = count;
			this.sum = sum;
		}
		
		@Override
		public void readFields( DataInput in ) throws IOException {
			count = in.readInt( );
			sum = in.readInt( );
		}

		@Override
		public void write(DataOutput data) throws IOException {
			data.writeInt(count);
			data.writeInt(sum);
		}
	}



    public static class OneGramMap extends Mapper<LongWritable, Text, Text, Pair> { 	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Text comb = new Text();
			Pair volumnPair = new Pair();
	    	String line = value.toString(); //define new variable to be string


			String[] inputs = line.split("\\s+");
		
			try {
	     	   Integer.parseInt(inputs[1]);
	     	   	String word = inputs[0].toLowerCase();

	        	if (word.contains("nu")){
	        		String final_value = inputs[1] +" "+ "nu";
	        		int vol = Integer.parseInt(inputs[3]);
	        		comb.set(final_value);
	        		volumnPair = new Pair( 1, vol);
	        		context.write(comb, volumnPair);
	        	}
	        
	        	if (word.contains("die")){
	        		String final_value = inputs[1] + " " + "die";
	        		int vol = Integer.parseInt(inputs[3]);
	        		comb.set(final_value);
	        		volumnPair = new Pair( 1, vol);
	        		context.write(comb, volumnPair);
	        	}
	        
	        	if (word.contains("kla")){
	        		String final_value = inputs[1] +" "+ "kla";
	        		int vol = Integer.parseInt(inputs[3]);
	        		comb.set(final_value);
	        		volumnPair = new Pair( 1, vol);
	        		context.write(comb, volumnPair);
	        	}

	    	} catch (NumberFormatException e) {
	      //do nothing
	    		}
			
    		}
    	}



	public static class TwoGramMap extends Mapper <LongWritable, Text, Text, Pair> { 
								//specify input and out keys
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Text comb = new Text();
		Pair volumnPair = new Pair();
	    String line = value.toString(); //define new variable to be string


		String[] inputs = line.split("\\s+");
		
		try {
	        Integer.parseInt(inputs[2]);
	        String word1 = inputs[0].toLowerCase();
	        String word2 = inputs[1].toLowerCase();
	        if (word1.contains("nu") || word2.contains("nu")){
	        	String final_value = inputs[2] +" "+ "nu";
	        	int vol = Integer.parseInt(inputs[4]);
	        	comb.set(final_value);
	        	volumnPair = new Pair( 1, vol);
	        	context.write(comb, volumnPair);
	        }
	        
	        if (word1.contains("die") || word2.contains("die")){
	        	String final_value = inputs[2] + " " + "die";
	        	int vol = Integer.parseInt(inputs[4]);
	        	comb.set(final_value);
	        	volumnPair = new Pair( 1, vol);
	        	context.write(comb, volumnPair);
	        }
	        
	        if (word1.contains("kla") || word2.contains("kla")){
	        	String final_value = inputs[2] +" "+ "kla";
	        	int vol = Integer.parseInt(inputs[4]);
	        	comb.set(final_value);
	        	volumnPair = new Pair( 1, vol);
	        	context.write(comb, volumnPair);
	        }

	    } catch (NumberFormatException e) {
	      //do nothing
	    	}
			
    	}
    }
    


    public static class myCombiner extends Reducer <Text, Pair, Text, Pair> {
	public void reduce(Text key, Iterable <Pair> values, Context context) throws IOException, InterruptedException {
		Pair volumnPair;
		int sum = 0;
	    int count= 0;

	    for (Pair value: values){
	    	sum += value.sum;
	    	count += value.count;
	    }  

	    volumnPair = new Pair(count, sum);
	    context.write(key,volumnPair);
		}
	}


    public static class Reduce extends Reducer <Text, Pair, Text, DoubleWritable> {
	public void reduce(Text key, Iterable <Pair> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
	    int count= 0;

	    for (Pair value: values){
	    	sum += value.sum;
	    	count += value.count;
	    }  
	    context.write(key,new DoubleWritable(sum / count));
		}
    }


    public static void main(String[] args) throws Exception {
	Path firstPath = new Path(args[0]);
	Path sencondPath = new Path(args[1]);
	Path outputPath = new Path(args[2]);

	Configuration conf = new Configuration();
	Job job = new Job(conf);

	job.setJarByClass(XiangLi1_exercise1.class);

	job.setReducerClass(Reduce.class);
	job.setCombinerClass(myCombiner.class);

        //output format for mapper
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Pair.class);

        //output format for reducer
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);

        //use MultipleOutputs and specify different Record class
	MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, OneGramMap.class);
	MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, TwoGramMap.class);
	FileOutputFormat.setOutputPath(job, outputPath);

	job.waitForCompletion(true);
	}
}

