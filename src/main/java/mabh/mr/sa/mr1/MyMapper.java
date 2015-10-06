package mabh.mr.sa.mr1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, ProductCostWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] arr = line.split("\t");

		String product = arr[0];
		String state = arr[1];
		String totalCost = arr[2];

		//using Text only
		//context.write(new Text(state), new Text(product + "_" + totalCost));
		
		//using Writable
		ProductCostWritable pcw = new ProductCostWritable();
		pcw.setProduct(product);
		pcw.setTotalCost(Float.parseFloat(totalCost));
		context.write(new Text(state), pcw);
	}
}
