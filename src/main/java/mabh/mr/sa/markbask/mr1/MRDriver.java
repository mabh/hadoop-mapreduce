package mabh.mr.sa.markbask.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	    job.setMapOutputValueClass( IntWritable.class );
	    
	    job.setOutputKeyClass( Text.class );	    
	    job.setOutputValueClass( IntWritable.class );
	    
	    FileInputFormat.addInputPath( job, new Path( otherArgs[0] ) );
	    FileOutputFormat.setOutputPath( job, new Path( otherArgs[1] ) );
	    
	    job.waitForCompletion( true );	   

	}


}

