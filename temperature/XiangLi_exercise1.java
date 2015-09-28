import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class XiangLi_exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> { 
	
//cannot use standard integer, java since in cluster serilized -> use IntWritable/DoubleWritable/...														//type of inpute and out keys of map funtion 
	//private final static IntWritable one = new IntWritable(1);
	private static Text Year = new Text();
	private static IntWritable Temp = new IntWritable();

	public void configure(JobConf job) {
	}
								//specify input and out keys
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString(); //define new variable to be string
		String check = line.substring(92, 93);	
		String year = line.substring(15, 19);
		String temp1 = line.substring(87, 92);
		//check if temperature value is positive (start with "+"), if yes, ignore "+" since mapreduce
		if (temp1.substring(0,1).equals("+")){ //check if temperature value is positive (start with "+"), if yes, ignore "+" since mapreduce 
		temp1 = temp1.substring(1,5);}
		int  type = Integer.parseInt(check);
		int temp = Integer.parseInt(temp1);
		
		if (Arrays.asList(0, 1, 4, 5, 9).contains(type) && temp != 9999){
			Year.set(year);
			Temp.set(temp);
			output.collect(Year, Temp); 
	    		}
			
    		}
	}
    

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    int maxTemp = -999;
	    while (values.hasNext()) {
	    int value = values.next().get();//get values
	    if (value > maxTemp){ maxTemp = value;}
	    else {maxTemp = maxTemp;}
	    }
	    output.collect(key, new IntWritable(maxTemp));
	}
    }


    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), XiangLi_exercise1.class);
	conf.setJobName("xiangli_exercise1");
//if change type, like IntWritable -> DoubleWritable, have to go to run() function change corresponding value
//example: subset of document, type is text
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new XiangLi_exercise1(), args);
	System.exit(res);
    }
}
