package de.tuberlin.dima.minidb.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;


public class G5InputSplit extends InputSplit implements Writable{
	
	private IntWritable firstPageNumber;
	private IntWritable lastPageNumber;
	private Text tableName;
	
	
	public G5InputSplit() {
		this.tableName = new Text("");
		this.firstPageNumber = new IntWritable(0);
		this.lastPageNumber = new IntWritable(0);
	}
	
	public G5InputSplit(String tableName, int firstPAgeNumber, int lastPageNumber) {
		this.tableName = new Text(tableName);
		firstPageNumber = new IntWritable(firstPAgeNumber);
		this.lastPageNumber = new IntWritable(lastPageNumber);
		
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lastPageNumber.get() - firstPageNumber.get();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new String[0];
	}
	
	
	
	public int getFirstPageNumber() {
		return this.firstPageNumber.get();
	}
	
	public int getLastPageNumber() {
		return this.lastPageNumber.get();
	}
	
	public String getTableName() {
		return this.tableName.toString();
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		
	  tableName.readFields(input);
	  firstPageNumber.readFields(input);
	  lastPageNumber.readFields(input);	  
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
	
		  tableName.write(output);
		  firstPageNumber.write(output);
		  lastPageNumber.write(output);	  				
	}
}
