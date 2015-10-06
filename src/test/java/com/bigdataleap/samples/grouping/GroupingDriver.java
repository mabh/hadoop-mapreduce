package com.bigdataleap.samples.grouping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * Commands for running this program
 * 
 * hadoop jar HadoopSamples.jar com.enablecloud.samples.sorting.SortingDriver retail/txn output/txnsort
 * 
 * where retail/txn is the input path and output/txnsort is the output path
 * 
 * copy the output to local filesystem
 * 
 * hadoop fs -copyToLocal /user/ubuntu/output/txnsort/part-r-00000 /home/ubuntu/work/samples/output/txnsort
 * 
 * Delete the HDFS output directory
 * 
 * hadoop fs -rmr /user/ubuntu/output/txnsort
 * 
 */

public class GroupingDriver {
	
	/**
	 * The main entry point.
	 */
	public static void main(String[] args) throws Exception {
	    
Configuration conf = new Configuration();
	  	
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  		    	    
	    Job job = Job.getInstance( conf );
	    job.setJobName( "Product Revenue");
	    
	    
	    job.setJarByClass( GroupingDriver.class );
	    job.setMapperClass( GroupingMapper.class );
	    job.setReducerClass( GroupingReducer.class );
	    
	    job.setMapOutputKeyClass( Text.class );
	    job.setMapOutputValueClass( DoubleWritable.class );
	    
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( Text.class );
	    
	    FileInputFormat.addInputPath( job, new Path( otherArgs[0] ) );
	    FileOutputFormat.setOutputPath( job, new Path( otherArgs[1] ) );

	    job.waitForCompletion( true );
    	
	}

}
