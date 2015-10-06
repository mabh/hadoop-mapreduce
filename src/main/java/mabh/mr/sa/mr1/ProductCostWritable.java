/*
* Copyright (c) 2013-2015 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package mabh.mr.sa.mr1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ProductCostWritable implements WritableComparable<ProductCostWritable> {

	private String product;
	private float totalCost;
	
	public ProductCostWritable() {}
	
	public void setProduct(String product) {
		this.product = product;
	}

	public void setTotalCost(float totalCost) {
		this.totalCost = totalCost;
	}

	public String getProduct() {
		return product;
	}

	public float getTotalCost() {
		return totalCost;
	}

	@Override
	public void readFields(DataInput dIn) throws IOException {
		this.product = dIn.readUTF();
		this.totalCost = dIn.readFloat();
	}

	@Override
	public void write(DataOutput dOut) throws IOException {
		dOut.writeUTF(this.product);
		dOut.writeFloat(this.totalCost);
	}

	@Override
	public int compareTo(ProductCostWritable o) {
		return this.getTotalCost() == o.getTotalCost() ? 0 : (this.getTotalCost() > o.getTotalCost() ? 1 : -1); 
	}
}
