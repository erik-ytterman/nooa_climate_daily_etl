package se.phaseshift.hadoop.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.server.UID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.io.Writable;

public class WritableGenericRecord implements Writable {
    private BinaryEncoder binaryEncoder;
    private BinaryDecoder binaryDecoder;

    protected GenericRecord record;
    
    public WritableGenericRecord() {}

    public WritableGenericRecord(GenericRecord record) {
	this.record = record;
    }
    
    public void setRecord(GenericRecord record) {
	this.record = record;
    }
    
    public GenericRecord getRecord() {
	return this.record;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
	// Write schema
	Schema schema = record.getSchema();
	String schemaString = schema.toString(false);
	out.writeUTF(schemaString);

	// Write data
	GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(schema);
	this.binaryEncoder = EncoderFactory.get().directBinaryEncoder((DataOutputStream) out, this.binaryEncoder);
	gdw.write(this.record, this.binaryEncoder);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	// Read schema
	Schema.Parser parser = new Schema.Parser();
	Schema schema = parser.parse(in.readUTF());

	// Read data
	GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>(schema);
	this.binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder((InputStream) in, null);
	this.record = gdr.read(this.record, this.binaryDecoder);
    }
}
