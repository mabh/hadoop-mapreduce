package com.bigdataleap.samples.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

public class MRUnitTestCase {
	
	   MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text> mapReduceDriver;
	   MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	   ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;

	   @Before
	   public void setUp() {
	      GroupingMapper mapper = new GroupingMapper();
	      GroupingReducer reducer = new GroupingReducer();
	      
	      mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
	      mapDriver.setMapper(mapper);
	      reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, Text>();
	      
	      reduceDriver.setReducer(reducer);
	      mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, Text>();
	      mapReduceDriver.setMapper(mapper);
	      mapReduceDriver.setReducer(reducer);
	   }

	   @Test
	   public void myTextTest() throws IOException {
		   Iterable<Text> it = Arrays.asList(new Text("a"), new Text("a"), new Text("c"));
		   String joined = Joiner.on(",")
					.skipNulls()
					.join(ImmutableSet.copyOf(it).asList());
		   System.out.println(joined);
	   }
	   
	   @Test
	   public void testMapper() throws IOException {
	      mapDriver.withInput( new LongWritable(1), 
	    		  new Text("00000000,01-03-2011,4006236,045.28,Outdoor Play Equipment" +
	    		  		",Sandboxes,NEW YORK,NEW YORK,credit") );
	      mapDriver.withOutput( new Text("NEW YORK"+"\t"+"SANDBOXES"), new DoubleWritable( 045.28 ) );
	      mapDriver.runTest();
	   }

	   @Test
	   public void testReducer() throws IOException {
	      List<DoubleWritable> values = new ArrayList<DoubleWritable>();
	      values.add( new DoubleWritable( 10.00 ) );
	      values.add( new DoubleWritable( 15.00 ) );
	      reduceDriver.withInput( new Text( "NEW YORK"+"\t"+"SANDBOXES" ), values );
	      reduceDriver.withOutput( new Text("NEW YORK"+"\t"+"SANDBOXES"), new Text( "25" ) );
	      reduceDriver.runTest();
	   }

	   @Test
	   public void testMapReduceSingleProduct() throws IOException {
	      mapReduceDriver.withInput( new LongWritable(1), 
	    		  new Text("00000000,01-03-2011,4006236,045.00,Outdoor Play Equipment," +
	    		  		"Sandboxes,NEW YORK,NEW YORK,credit") );
	      mapReduceDriver.withInput( new LongWritable(1), 
	    		  new Text("00000000,01-03-2011,4006236,045.00,Outdoor Play Equipment," +
	    		  		"Sandboxes,NEW YORK,NEW YORK,credit") );
	      mapReduceDriver.addOutput( new Text("NEW YORK"+"\t"+"SANDBOXES"), new Text( "90" ) );
	      mapReduceDriver.runTest();
	   }
	   
	   @Test
	   public void testMapReduceMultipleProducts() throws IOException {
	      mapReduceDriver.withInput( new LongWritable(1), new Text("00000000,01-03-2011,4006236,045.00,Outdoor Play Equipment,Sandboxes,NEW YORK,NEW YORK,credit") );
	      mapReduceDriver.withInput( new LongWritable(1), new Text("00000000,01-03-2011,4006236,045.00,Outdoor Play Equipment,Skating,NEW YORK,NEW YORK,credit") );
	      mapReduceDriver.addOutput( new Text("NEW YORK"+"\t"+"SANDBOXES"), new Text( "45" ) );
	      mapReduceDriver.addOutput( new Text("NEW YORK"+"\t"+"SKATING"), new Text( "45" ) );	      
	      mapReduceDriver.runTest();
	   }

}
