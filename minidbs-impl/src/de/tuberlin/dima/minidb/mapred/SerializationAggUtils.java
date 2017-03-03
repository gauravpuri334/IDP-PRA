package de.tuberlin.dima.minidb.mapred;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.IntWritable;

import de.tuberlin.dima.minidb.core.BasicType;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.parser.OutputColumn.AggregationType;



public class SerializationAggUtils {
	
	public static void writeAggTypeToStream(AggregationType aggFunction, 
			DataOutput out) throws IOException {
		
		(new IntWritable(aggFunction.ordinal())).write(out);

	}
	
	
	public static String writeAggTypeArrayToString(AggregationType[] aggFunctions) throws IOException {
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(4 + aggFunctions.length * 4);
		DataOutputStream out = new DataOutputStream(byte_stream);
		
		out.writeInt(aggFunctions.length);
		for (int i=0; i < aggFunctions.length; ++i) {
			SerializationAggUtils.writeAggTypeToStream(aggFunctions[i], out);
		}		
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	
	public static AggregationType[] readAggTypeArrayFromString(String s) throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		AggregationType[] array = new AggregationType[in.readInt()];
		
		for (int i=0; i<array.length; ++i) {
			
			array[i] = AggregationType.values()[in.readInt()];
		}
		
		return array;
	}
	
	
	public static void writeDataTypeToStream(DataType type, 
			DataOutput out) throws IOException {
		
		(new IntWritable(type.getBasicType().ordinal())).write(out);
		(new IntWritable(type.getLength())).write(out);

	}
	
	
	public static String writeDataTypeArrayToString(DataType[] types) throws IOException {
		
		ByteArrayOutputStream byte_stream = new ByteArrayOutputStream(4 + types.length * 8);
		DataOutputStream out = new DataOutputStream(byte_stream);
		
		out.writeInt(types.length);
		for (int i=0; i < types.length; ++i) {

			SerializationAggUtils.writeDataTypeToStream(types[i], out);
		}		
		return new String(Base64.encodeBase64(byte_stream.toByteArray()));
	}
	
	
	public static DataType[] readDataTypeArrayFromString(String s) throws IOException {
		if (s == null || s.isEmpty()) return null;
		DataInput in = new DataInputStream(
				new ByteArrayInputStream(Base64.decodeBase64(s)));
		
		DataType[] array = new DataType[in.readInt()];
		
		
		
		for (int i=0; i<array.length; ++i) {
			
			int type = in.readInt();
			int length = in.readInt();
			
			if (type == 10)
				array[i] = DataType.ridType();
			else
				array[i] = DataType.get(BasicType.values()[type], length);
			
		}
		
		return array;
	}


}
