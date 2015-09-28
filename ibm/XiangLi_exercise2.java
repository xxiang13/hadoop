import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class XiangLi_exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> { 
	
//cannot use standard integer, java since in cluster serilized -> use IntWritable/DoubleWritable/...														//type of inpute and out keys of map funtion 
	//private final static IntWritable one = new IntWritable(1);
	private static Text Comb = new Text();
	private static DoubleWritable Num = new DoubleWritable();

	public void configure(JobConf job) {
	}
								//specify input and out keys
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    String line = value.toString(); //define new variable to be string

	 //object created outside of Map function, Word onject only needs to be defined once, don't have to define each time in while loop -> save memory
	 // instead of each time: Text word = new Text(), use word.set(..): change/update the value;

		int num_comma = 0;
		int i = 0;
		int len = line.length();
		if (line.substring(len-5,len).equals("false")){ //check if last column is "false"
			while (num_comma <=34){
			 // while loop is to save memory: not saving each column since we just need first 33 column, count 34 commas and will give 33 columns
			 // i will return 34th comma's location in the string
				if(line.substring(i,i+1).equals(",")){
					num_comma += 1;
					i +=1;
				}
				else {
					i += 1;
				}
			}
			String line_new = line.substring(0, i-1); // when doing substring, not including 34th comma 
			String[] inputs = line_new.split(","); //split the string by comma
			Double num1 = Double.parseDouble(inputs[3]); //get the 4th column's value
			Num.set(num1);
			
			StringBuilder builder = new StringBuilder();
			for(int a=29;a <= 32;a++) { //get column 30 to 33's values and append it each time
			    builder.append(inputs[a].substring(0,1));
				}
			String comb1 = builder.toString(); // convert to string
			Comb.set(comb1);

			output.collect(Comb, Num); 
	    		}
			
    	}
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	    double sum = 0.0;
	    double  b= 0.0;
	    while (values.hasNext()) {
	    b += 1.0; //count how many the combination showing up
	    sum += values.next().get(); // sum values for a same combination
	    }
	    Double ave = sum / b ; // get the average value
	    output.collect(key, new DoubleWritable(ave));
	}
    }


    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), XiangLi_exercise2.class);
	conf.setJobName("xiangli_exercise2");
//if change type, like IntWritable -> DoubleWritable, have to go to run() function change corresponding value
//example: subset of document, type is text
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);

	conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class); //some error, TA suggests comment this line, will learn in class
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new XiangLi_exercise2(), args);
	System.exit(res);
    }
}
