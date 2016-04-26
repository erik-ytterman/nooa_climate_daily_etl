package se.phaseshift.hadoop.climate.daily;

import java.lang.InterruptedException;

import java.io.IOException;
import java.io.StringReader;
import java.io.PrintWriter;
import java.io.StringWriter;

// MapReduce & Hadoop
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// AVRO
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

// AVRO UTILS
import se.phaseshift.hadoop.util.WritableGenericRecord;

// Parquet
import org.apache.parquet.Log;

// Logging
import org.apache.log4j.Logger;

// XXX Generic record does not implement Writable, thus this will fail!
// XXX http://stackoverflow.com/questions/22135566/not-understanding-a-mapreduce-npe
public class JsonlDailyETLReducer extends Reducer<Text, WritableGenericRecord, Void, GenericRecord> {
    private MultipleOutputs outputStreams = null;

    @Override
    public void setup(Context context) {
	// Get configuration
	Configuration conf = context.getConfiguration();
	conf.setBoolean("mapred.output.compress", false);
	
	// Create multiple outputs 
	this.outputStreams = new MultipleOutputs(context);

	/*
	try {
	    this.outputStreams.write("errors", NullWritable.get(), new Text("REDUCE"), "errors/reduction");
	}
	catch(IOException ioe) {
	    System.err.println(ioe);
	}
	catch(InterruptedException ie) {
	    System.err.println(ie);
	}
	*/
    }

    @Override
    public void reduce(Text key, Iterable<WritableGenericRecord> records, Context context) throws IOException, InterruptedException {
	for(WritableGenericRecord record: records) {
	    // Dispatch data
	    // context.write(null, record.getRecord());
	}
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	// Close multiple outputs!
	this.outputStreams.close();
    }
}
