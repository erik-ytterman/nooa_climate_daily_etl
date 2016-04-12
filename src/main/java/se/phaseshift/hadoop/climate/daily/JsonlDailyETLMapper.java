package se.phaseshift.hadoop.climate.daily;

import java.lang.InterruptedException;

import java.io.IOException;
import java.io.StringReader;
import java.io.PrintWriter;
import java.io.StringWriter;

// JSON parser
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// JSON Schema validator
import com.github.fge.jsonschema.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.github.fge.jsonschema.report.ProcessingMessage;

// MapReduce & Hadoop
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;

// AVRO
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericRecord;

// Parquet
import org.apache.parquet.Log;

// Logging
import org.apache.log4j.Logger;

public class JsonlDailyETLMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
    private GenericRecordBuilder recordBuilder = null;
    private ObjectMapper objectMapper = null;
    private JsonSchema inputSchema = null;
    private MultipleOutputs<LongWritable, Text> mos = null;

    @Override
    public void setup(Context context) {
	// Get configuration
	Configuration conf = context.getConfiguration();
	conf.setBoolean("mapred.output.compress", false);
	
	// Create multiple outputs 
	mos = new MultipleOutputs(context);

	// Create an Jackson Object mapper needed for JSON parsing
	this.objectMapper = new ObjectMapper();
	
	try {
	    // Create a JSON input schema used as input validator
	    JsonNode schemaNode = this.objectMapper.readTree(conf.get("climate.stations.input.schema"));
	    this.inputSchema = JsonSchemaFactory.byDefault().getJsonSchema(schemaNode);

	    // Create a record builder for output (AVRO) records
	    Schema outputSchema = new Schema.Parser().parse(conf.get("climate.stations.output.schema"));
	    this.recordBuilder = new GenericRecordBuilder(outputSchema);
	}
	catch(Exception e) {
	    System.err.println(e.toString());
	}
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	try {
	    // Parse JSON line data into JsonNode
	    JsonNode jsonNode = this.objectMapper.readTree(value.toString());
	    
	    // Validate against schema
	    this.validateJsonSchema(jsonNode);
	    
	    // Extract data from JSON line instance 	
	    String stationId        = jsonNode.get("id").asText();
	    Float  stationLatitude  = new Float(jsonNode.get("latitude").asDouble());
	    Float  stationLongitude = new Float(jsonNode.get("longitude").asDouble());
	    Float  stationElevation = new Float(jsonNode.get("elevation").asDouble());
	    String stationName      = jsonNode.get("name").asText();
	    
	    // Extract MapReduce meta-data potentially used in KPI calculation
	    FileSplit fileSplit   = (FileSplit) context.getInputSplit();	
	    String fileName         = fileSplit.getPath().getName();
	    
	    // Configre generic AVRO record output data
	    this.recordBuilder.set("id", stationId);
	    this.recordBuilder.set("latitude" , stationLatitude);
	    this.recordBuilder.set("longitude", stationLongitude);
	    this.recordBuilder.set("elevation", stationElevation);
	    this.recordBuilder.set("name", stationName);
	    
	    // Generate AVRO record
	    GenericRecord record = this.recordBuilder.build();

	    // Dispatch data		
	    context.write(null, record);
	}
	catch(JsonProcessingException jpe) {
	    this.mos.write("error", key, value);

	    this.mos.write("error", key, this.removeLineBreak(jpe.getMessage()));
	}
	catch(JsonlDailyValidationException jve) {
	    this.mos.write("error", key, value);

	    for(ProcessingMessage pm : jve) {
		this.mos.write("error", key, this.removeLineBreak(pm.toString()));	
	    }
	}
	catch(Exception e) {
	    this.mos.write("error", key, this.removeLineBreak(e.getMessage()));

	    for(StackTraceElement ste : e.getStackTrace()) {
		this.mos.write("error", key, this.removeLineBreak(ste.toString()));		
	    }
	}
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	// Close multiple outputs!
	this.mos.close();
    }

    private boolean validateJsonSchema(JsonNode jsonNode) throws Exception, JsonlDailyValidationException {
	ProcessingReport validationReport = this.inputSchema.validate(jsonNode);
	if(!validationReport.isSuccess()) { 
	    throw new JsonlDailyValidationException(validationReport); 
	}

	return true;
    }
    
    private String removeLineBreak(String text) {
	return text.replace("\n", "").replace("\r", "");
    }
}
