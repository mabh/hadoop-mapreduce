package mabh.mr.sa.markbask.mr0;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] arr = line.split(",");

		String date = arr[1];
		String customer = arr[2];
		String concat = date + "_" + customer; 		
		String product = arr[5];
		
		context.write(new Text(concat), new Text(product));
	}
}
