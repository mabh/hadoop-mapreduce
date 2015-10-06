/*
* Copyright (c) 2013-2015 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package mabh.mr.sa.partitioner;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MRQuarterPartitioner extends Partitioner<Text, FloatWritable> {
	@Override
	public int getPartition(Text arg0, FloatWritable arg1, int numPartitions) {
		//arg0 is date Text - find quarter - that is the partition
		if("Q1".equals(arg0.toString())) {
			return 0;
		} else if("Q2".equals(arg0.toString())) {
			return 1;
		} else if("Q3".equals(arg0.toString())) {
			return 2;
		} else if("Q4".equals(arg0.toString())) {
			return 3;
		}
		return 0;
	}
}
