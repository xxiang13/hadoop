import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class XiangLi1_exercise3 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 
	
//cannot use standard integer, java since in cluster serilized -> use IntWritable/DoubleWritable/...														//type of inpute and out keys of map funtion 
	//private final static IntWritable one = new IntWritable(1);
	private static Text Final_Value = new Text();
	private static final Text dummy = new Text("");

	public void configure(JobConf job) {
	}
								//specify input and out keys
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    String line = value.toString(); //define new variable to be string

	    ArrayList<Integer> range = new ArrayList<Integer>();
		for (int i = 2000; i <= 2010; i++ ){
			range.add(i);
		}

		//String[] inputs = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		String[] inputs = line.split(",");

		try {
		
		int year = Integer.parseInt(inputs[165]);	
			
		if (range.contains(year)){
			String dur = inputs[3];
			String artist_name = inputs[2];
			String song_title = inputs[1];
			String final_input = artist_name + ','+dur+','+song_title; 
			Final_Value.set(final_input);
			output.collect(Final_Value, dummy); 
		     }
			}

		catch (NumberFormatException e) {
			//do nothing
			}
	    }
			
    	}


    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	   
	    }
    }


    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), XiangLi1_exercise3.class);
	conf.setJobName("xiangli1_exercise3");
	conf.setNumReduceTasks(0);
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	//conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new XiangLi1_exercise3(), args);
	System.exit(res);
    }
}
