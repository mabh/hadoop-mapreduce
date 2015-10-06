package mabh.mr.wc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.inject.Inject;

public class SkeletonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Inject MapLogic<LongWritable, Text, Text, IntWritable> mapLogic;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<TuplePair<Text, IntWritable>> list = mapLogic.execute(key, value);
		for(TuplePair<Text, IntWritable> element: list) {
			context.write(element.getFirst(), element.getSecond());
		}
	}
}
