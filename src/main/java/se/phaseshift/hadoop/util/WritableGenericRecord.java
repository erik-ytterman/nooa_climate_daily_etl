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
    protected Schema schema;
    
    public WritableGenericRecord() {}

    public WritableGenericRecord(GenericRecord record, Schema schema) {
	this.record = record;
	this.schema = schema;
    }
    
    public void setRecord(GenericRecord record) {
	this.record = record;
    }
    
    public GenericRecord getRecord() {
	return record;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
	GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>(this.schema);
	this.binaryEncoder = EncoderFactory.get().directBinaryEncoder((DataOutputStream) out, this.binaryEncoder);

	gdw.write(this.record, this.binaryEncoder);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	// GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>(this.schema);
	// GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();
	// this.binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder((InputStream) in, this.binaryDecoder);

        // this.record = new GenericData.Record(this.schema);
	// this.record = gdr.read(this.record, this.binaryDecoder);
    }
}
