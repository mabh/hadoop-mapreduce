package mabh.mr.wc;

import java.io.IOException;
import java.util.stream.StreamSupport;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
    int sum = StreamSupport
    	.stream(values.spliterator(), false)
    	.mapToInt(value -> value.get())
    	.sum();
    context.write(key, new IntWritable(sum));
  }
}