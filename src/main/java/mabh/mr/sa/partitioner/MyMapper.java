package mabh.mr.sa.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split(",");
		String date = arr[1];
		
		String dateArr[] = date.split("-");
		int month = Integer.parseInt(dateArr[0]) - 1;
		int quarter = (month / 3) + 1;
		
		context.write(new Text("Q" + quarter), new FloatWritable(Float.parseFloat(arr[3])));
	}
}
