package mabh.mr.sa.mr0;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	enum TXN_COUNTERS {
		HIGH_VALUE_TXNS
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split(",");
		String state = arr[7];
		String product = arr[5];
		String amount = arr[3];
		
		if(Float.parseFloat(amount) > 100) {
			context.getCounter(TXN_COUNTERS.HIGH_VALUE_TXNS).increment(1);
		}
		
		context.write(new Text(product + "\t" + state), new FloatWritable(Float.parseFloat(arr[3])));
	}
}
