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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.lang.Math;
import java.lang.Integer;


public class XiangLi1_exercise2 {


	public static class Triple implements Writable {

	// An implementation of value with tag, as a writable object

		private long count;
		private long sum;
		private long sum_sqr;

		public Triple( ) {
			long count = 0;
			long sum = 0;
			long sum_sqr = 0;
		}

		public Triple( long count, long sum, long sum_sqr ) {
			this.count = count;
			this.sum = sum;
			this.sum_sqr = sum_sqr;
		}
		
		@Override
		public void readFields( DataInput in ) throws IOException {
			count = in.readLong( );
			sum = in.readLong( );
			sum_sqr = in.readLong();
		}

		@Override
		public void write(DataOutput data) throws IOException {
			data.writeLong(count);
			data.writeLong(sum);
			data.writeLong(sum_sqr);
		}
	}



    public static class OneGramMap extends Mapper<LongWritable, Text, Text, Triple> { 	
    	private final static Text comb = new Text("");

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Triple volumnTriple = new Triple();
	    	String line = value.toString(); //define new variable to be string


			String[] inputs = line.split("\\s+");
		
			try {
	     	   //Integer.parseInt(inputs[1]);
	        	
	           long vol = Integer.parseInt(inputs[3]);
	           long vol_sqr = vol*vol;
	           volumnTriple = new Triple( 1, vol, vol_sqr);
	           context.write(comb, volumnTriple);
	        
	    	} catch (NumberFormatException e) {
	      //do nothing
	    		}
			
    		}
    	}



	public static class TwoGramMap extends Mapper <LongWritable, Text, Text, Triple> { 
		private final static Text comb = new Text("");
								//specify input and out keys
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		Triple volumnTriple = new Triple();
	    String line = value.toString(); //define new variable to be string


		String[] inputs = line.split("\\s+");
		
		try {
	        //Integer.parseInt(inputs[2]);
	        long vol = Integer.parseInt(inputs[4]);
	        long vol_sqr = vol*vol;
	        volumnTriple = new Triple( 1, vol, vol_sqr);
	        context.write(comb, volumnTriple);
	        

	    } catch (NumberFormatException e) {
	      //do nothing
	    	}
			
    	}
    }
    


    public static class myCombiner extends Reducer <Text, Triple, Text, Triple> {
	public void reduce(Text key, Iterable <Triple> values, Context context) throws IOException, InterruptedException {
		Triple volumnTriple;
		long sum = 0;
	    long count= 0;
	    long sum_sqr = 0;

	    for (Triple value: values){
	    	sum += value.sum;
	    	count += value.count;
	    	sum_sqr += value.sum_sqr;
	    } 

	    volumnTriple = new Triple(count, sum, sum_sqr);
	    context.write(key,volumnTriple);
		}
	}


    public static class Reduce extends Reducer <Text, Triple, Text, DoubleWritable> {
	public void reduce(Text key, Iterable <Triple> values, Context context) throws IOException, InterruptedException {
		double sum = 0;
	    long count= 0;
	    double sum_sqr = 0; 

	    for (Triple value: values){
	    	sum += value.sum;
	    	count += value.count;
	    	sum_sqr += value.sum_sqr;
	    }  
	    double std = Math.sqrt(sum_sqr/count - (sum*sum)/(count*count));
	    DoubleWritable stdVolumes = new DoubleWritable( );
	    stdVolumes.set(std);
	    //String val = String.valueOf(sum) + "|" + String.valueOf(count) + "|" + String.valueOf(sum_sqr);

	    context.write(key,stdVolumes);
		}
    }


    public static void main(String[] args) throws Exception {
	Path firstPath = new Path(args[0]);
	Path sencondPath = new Path(args[1]);
	Path outputPath = new Path(args[2]);

	Configuration conf = new Configuration();
	Job job = new Job(conf);

	job.setJarByClass(XiangLi1_exercise2.class);
	job.setJobName("xiangli1_exercise2");

	job.setReducerClass(Reduce.class);
	job.setCombinerClass(myCombiner.class);
        //output format for mapper
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Triple.class);

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

