package mabh.mr.sa.markbask.mr0;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.base.Joiner;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String joined = Joiner.on(",")
			.skipNulls()
			.join(values);
			//.join(ImmutableSet.copyOf(values).asList());
				/*
				 * Trampoline Accessories,Jump Ropes,Yoga & Pilates
				 * is converted into
				 * Yoga & Pilates only when set is used ???
				 */
		//date1_cust1 => (p1, p2, p3, ... , pn) [unique products]
		context.write(key, new Text(joined));
	}
}