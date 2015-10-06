package mabh.mr.sa.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
 
public class MRDriver {
 
	public static void main(String[] args) throws Exception {
		
	  	Configuration conf = new Configuration();
	  	
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  		    	    
	    Job job = Job.getInstance( conf );
	    job.setJobName( "Word Count");
	    
	    job.setJarByClass( MRDriver.class );
	    job.setMapperClass( MyMapper.class );
	    job.setReducerClass( MyReducer.class );
	    
	    job.setMapOutputKeyClass( Text.class );
	    job.setMapOutputValueClass( FloatWritable.class );
	    
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( FloatWritable.class );
	    
	    job.setPartitionerClass(MRQuarterPartitioner.class);
	    job.setNumReduceTasks(4);
	    
	    FileInputFormat.addInputPath( job, new Path( otherArgs[0] ) );
	    FileOutputFormat.setOutputPath( job, new Path( otherArgs[1] ) );
	    
	    job.waitForCompletion( true );	   

	}


}

