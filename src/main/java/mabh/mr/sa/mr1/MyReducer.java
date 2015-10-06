package mabh.mr.sa.mr1;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Ordering;

/*class PT implements Comparable<PT> {
	private String product;
	private float totalcost;
	public PT(String product, float totalcost) {
		this.product = product;
		this.totalcost = totalcost;
	}
	
	public String getProduct() {
		return this.product;
	}
	
	public float getTC() {
		return this.totalcost;
	}
	
	@Override
	public int compareTo(PT o) {
		return this.getTC() == o.getTC() ? 0 : (this.getTC() > o.getTC() ? 1 : -1); 
	}
}*/

/*public class MyReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<PT> list = new ArrayList<>();
		for (Text val : values) {
			String product_totalcost = val.toString();
			String arr[] = product_totalcost.split("_");
			String product = arr[0];
			float totalcost = Float.parseFloat(arr[1]);
			list.add(new PT(product, totalcost));
		}

		Collections.sort(list);
		
		context.write(key, new Text(list.get(0).getProduct() + "-" + list.get(0).getTC()));
	}
}*/

public class MyReducer extends Reducer<Text, ProductCostWritable, Text, Text> {
	public void reduce(Text key, Iterable<ProductCostWritable> values, Context context) throws IOException, InterruptedException {
		List<ProductCostWritable> list = Ordering.natural().sortedCopy(values);
		context.write(key, new Text(list.get(0).getProduct() + "-" + list.get(0).getTotalCost()));
	}
}




