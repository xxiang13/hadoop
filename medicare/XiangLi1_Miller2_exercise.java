import java.io.*;
import java.util.*;
import java.lang.*;

// import hadoop io fomrmat
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

// import hadoop setup
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

// import hadoop types
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// import hadoop classes
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// import disrtriubted cahe
import org.apache.hadoop.filecache.DistributedCache;
//import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;
import java.net.URI; 

import org.apache.hadoop.io.FloatWritable; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.fs.FSDataInputStream; 
import org.apache.hadoop.mapreduce.RecordReader; 
import java.io.File;
import java.util.Scanner; 
import org.apache.hadoop.io.IOUtils; 


public class XiangLi1_Miller2_exercise {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> { 	

    	private Text cluster = new Text();
		private Text input = new Text();
		

		//private Text va = new Text();
		//private Text ke = new Text();

    	private HashMap<String, ArrayList<String>> centroids = new HashMap<String,ArrayList<String>>();
    	private File CentroidList ; 
	    private Scanner opnScanner ; 


	    
	    protected void setup(Context context) throws IOException, InterruptedException {
	       CentroidList = new File("centroids");
	       Scanner opnScanner = new Scanner(CentroidList); 
	       while(opnScanner.hasNextLine()) { 
	       		String s = opnScanner.nextLine();
	       		ArrayList<String> list = new ArrayList<String>(Arrays.asList(s.split("\\s+")));
				String cluster = list.get(0); 
				list.remove(0);
	       		centroids.put(cluster, list);			
			}
	    }
	    



		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    	String line = value.toString(); //define new variable to be string
			String[] inputs = line.split("\\s+");
		
			int size = inputs.length;


			int clust = 8;
			double minDist = 100000.0;

			for (int c=1; c < centroids.size()+1; c++){

				String cen = " ";
				double sum = 0.0;		
				//int clust = 0;
			
				
				for (int i = 0; i< size; i++) {
					double a = Double.parseDouble(inputs[i]) - Double.parseDouble(centroids.get(String.valueOf(c)).get(i));
					double dis_sq = a * a;
					sum = sum + dis_sq;	
					cen = cen + centroids.get(String.valueOf(c)).get(i);
				}

				double dis = Math.sqrt(sum);
				
	    		if (dis < minDist ){ 
	    			minDist = dis;

	    			clust = c;
	    		}
	   	 		else {
	   	 			minDist = minDist;
				}

				/*
				//String clut = "assign cluster:"+String.valueOf(clust) + "/distance:" + String.valueOf(dis) + "/min dist:" + String.valueOf(minDist);
				String clut = "cluster:"+String.valueOf(c) + "  centroid:" + String.valueOf(centroids.get(String.valueOf(c)));
				input.set(line);
				cluster.set(clut);
				context.write(cluster, input);
				*/
				
				
			}		
				

			//String clut = "assign cluster:"+String.valueOf(clust) + " min dist:" + String.valueOf(minDist);
			//String clut = Arrays.toString(arr);
			//String[] arr = centroids.get(1).toArray(new String[centroids.get(1).size()]);

			
			String clut = String.valueOf(clust);
			input.set(line);
			cluster.set(clut);
			context.write(cluster, input);
			
			
			

			
		}
    }


    public static class Reduce extends Reducer <Text, Text, Text, Text> {
    	private Text cluster = new Text();
	    private Text new_v = new Text();
	    private HashMap<Integer, Double> sum_cor = new HashMap<Integer, Double>();
	    private HashMap<Integer, Double> new_centroid = new HashMap<Integer, Double>();
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


			/*
			for (Text value:values) 
			{
				context.write( key, value);
				
			}
			*/
		
		int count= 0;
	    
	    
	    //double[] sum_cor = new double[3];
	    //double[] new_centroid = new double[3];
	    int dim = 0;
	    
	    double sum = 0.0;

		for (Text value:values) 
			{
				
				String line = value.toString();
				String[] input = line.split("\\s+");
				dim = input.length;

	    		for (int i = 0; i< dim; i++) {
	    			double a = 0.0;
					a = Double.parseDouble(input[i]);

					if (sum_cor.containsKey(i)){
					 sum_cor.put(i,sum_cor.get(i)+a); 
					}
					
					else{ 
						sum_cor.put(i,a) ;
					}
					
					
				}

				count ++;
			}



		String new_centro= "";

		for (int i = 0; i< dim; i++) {
		new_centroid.put(i,sum_cor.get(i) / count);	
		new_centro = new_centro + " "+ String.valueOf(new_centroid.get(i));

		//new_centro = new_centro + " "+ String.valueOf(new_centroid.get(i))+" " + String.valueOf(sum_cor.get(i)) + "Count:"+ String.valueOf(count);

		}

		//String new_centro = Arrays.toString(new_centroid);
	    context.write(key,new Text(new_centro));
	    

			}
		}
			    
		
    


    public static void main(String[] args) throws Exception {

	Configuration conf = new Configuration();
	Job job = new Job(conf);

	job.setJarByClass(XiangLi1_Miller2_exercise.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setJobName("xiangli1_miller2_exercise");
	//job.setCombinerClass(myCombiner.class);

	URI partitionUri = new URI("/user/huser75/DC/centroid.txt#centroids");
    DistributedCache.addCacheFile(partitionUri,job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());

        //output format for mapper
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

        //output format for reducer
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

        //use MultipleOutputs and specify different Record class
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

