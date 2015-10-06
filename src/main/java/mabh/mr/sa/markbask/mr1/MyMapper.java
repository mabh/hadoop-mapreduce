package mabh.mr.sa.markbask.mr1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split("\t");
		String productLine = arr[1];
		String[] products = productLine.split(",");
		
		for(int i = 0; i < products.length; i++) {
			for(int j = i + 1; j < products.length; j++) {
				String p1 = products[i];
				String p2 = products[j];
				String firstPos = "";
				String secondPos = "";
				if(p1.equalsIgnoreCase(p2)) {
					continue;
				}
				if(p1.compareTo(p2) == -1) {	//p1 < p2
					firstPos = p1;
					secondPos = p2;
				} else {
					firstPos = p2;
					secondPos = p1;
				}
				context.write(new Text(firstPos + "-" + secondPos), new IntWritable(1));
			}
		}
	}
}
