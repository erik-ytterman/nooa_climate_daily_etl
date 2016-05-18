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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// AVRO
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericRecord;

// AVRO UTILS
import se.phaseshift.hadoop.util.WritableGenericRecord;

// Parquet
import org.apache.parquet.Log;

// Logging
import org.apache.log4j.Logger;

public class JsonlDailyETLMapper extends Mapper<LongWritable, Text, Text, WritableGenericRecord> {
    private GenericRecordBuilder recordBuilder = null;
    private ObjectMapper objectMapper = null;
    private JsonSchema inputSchema = null;
    private Schema outputSchema = null;
    private MultipleOutputs outputStreams = null;

    /*---------------------------------------------------------------------------------------------------*/
    /* MAPPER IMPLEMENTATION                                                                             */
    /*---------------------------------------------------------------------------------------------------*/    

    @Override
    public void setup(Context context) {
	// Get configuration
	Configuration conf = context.getConfiguration();
	conf.setBoolean("mapred.output.compress", false);
	
	// Create multiple outputs 
	this.outputStreams = new MultipleOutputs(context);

	// Create an Jackson Object mapper needed for JSON parsing
	this.objectMapper = new ObjectMapper();
	
	try {
	    // Create a JSON input schema used as input validator
	    JsonNode schemaNode = this.objectMapper.readTree(conf.get("climate.stations.input.schema"));
	    this.inputSchema = JsonSchemaFactory.byDefault().getJsonSchema(schemaNode);

	    // Create a record builder for output (AVRO) records
	    this.outputSchema = new Schema.Parser().parse(conf.get("climate.stations.output.schema"));
	    this.recordBuilder = new GenericRecordBuilder(this.outputSchema);
	}
	catch(Exception e) {
	    System.out.println(e.toString());
	}
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	try {
	    // Parse JSON line data into JsonNode
	    JsonNode jsonNode = this.parseJsonInstance(value.toString());
	    
	    // Validate against schema
	    this.validateJsonInstance(jsonNode);
	    
	    // Extract data from JSON line instance 	
	    String  dailyId    = jsonNode.get("id").asText();
	    Integer dailyYear  = new Integer(jsonNode.get("year").asInt());
	    Integer dailyMonth = new Integer(jsonNode.get("month").asInt());
	    Integer dailyDay   = new Integer(jsonNode.get("day").asInt());
	    Float   dailyValue = new Float(jsonNode.get("value").asDouble());
	    
	    // Extract MapReduce meta-data potentially used in KPI calculation
	    FileSplit fileSplit   = (FileSplit) context.getInputSplit();	
	    String fileName       = fileSplit.getPath().getName();
	    
	    // Configre generic AVRO record output data
	    this.recordBuilder.set("id"   , dailyId);
	    this.recordBuilder.set("year" , dailyYear);
	    this.recordBuilder.set("month", dailyMonth);
	    this.recordBuilder.set("day"  , dailyDay);
	    this.recordBuilder.set("value", dailyValue);

	    // Generate AVRO record and wrap it to be writable
	    WritableGenericRecord record = new WritableGenericRecord(this.recordBuilder.build());

	    // Dispatch data		
	    context.write(new Text(dailyYear.toString()), record);
	}
	catch(JsonProcessingException jpe) {
	    this.writeParserError(value, jpe);
	}
	catch(JsonlDailyValidationException jve) {
	    this.writeValidationError(value, jve);
	}
	catch(Exception e) {
	    this.writeFrameworkError(e);
	}
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
	// Close multiple outputs!
	this.outputStreams.close();
    }

    /*---------------------------------------------------------------------------------------------------*/
    /* PARSING HELPERS                                                                                   */
    /*---------------------------------------------------------------------------------------------------*/

    private JsonNode parseJsonInstance(String jsonString) throws Exception, JsonProcessingException {
	return this.objectMapper.readTree(jsonString);
    }

    private void writeParserError(Text value, JsonProcessingException jpe) throws IOException, InterruptedException {
	this.outputStreams.write("parsing", 
				 NullWritable.get(), 
				 value, 
				 "errors/parsing");
    }

    /*---------------------------------------------------------------------------------------------------*/
    /* VALIDATION HELPERS                                                                                */
    /*---------------------------------------------------------------------------------------------------*/
    
    private boolean validateJsonInstance(JsonNode jsonNode) throws Exception, JsonlDailyValidationException {
	ProcessingReport validationReport = this.inputSchema.validate(jsonNode);
	if(!validationReport.isSuccess()) { 
	    throw new JsonlDailyValidationException(validationReport); 
	}

	return true;
    }
    
    private void writeValidationError(Text value, JsonlDailyValidationException jve) throws IOException, InterruptedException {
	this.outputStreams.write("validation", NullWritable.get(), value, "errors/validation");
	
	for(ProcessingMessage pm : jve) {
	    this.outputStreams.write("validation", 
				     NullWritable.get(), 
				     this.removeLineBreak(pm.toString()),
				     "errors/validation");	
	}
    }

    /*---------------------------------------------------------------------------------------------------*/
    /* FRAMEWORK HELPERS                                                                                 */
    /*---------------------------------------------------------------------------------------------------*/   

    private void writeFrameworkError(Exception e) throws IOException, InterruptedException {
	this.outputStreams.write("framework", 
				 NullWritable.get(), 
				 new Text(this.removeLineBreak(e.getMessage())), 
				 "errors/framework");
	    
	for(StackTraceElement ste : e.getStackTrace()) {
	    this.outputStreams.write("framework", 
				     NullWritable.get(), 
				     new Text(this.removeLineBreak(ste.toString())), 
				     "errors/framework");
	}
    }

    /*---------------------------------------------------------------------------------------------------*/
    /* GENERIC HELPERS                                                                                   */
    /*---------------------------------------------------------------------------------------------------*/

    private String removeLineBreak(String text) {
	String result = "NOTHING";

	if(text == null)
	    result = "NOTHING";
	else
	    result = text.replace("\n", "").replace("\r", ""); 

	return result;
    }
}
