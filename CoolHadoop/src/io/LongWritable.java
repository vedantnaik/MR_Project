package io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
/**
 * 
 * @author Vedant_Naik, Vaibhav_Tyagi, Dixit_Patel, Rohan_Joshi
 *
 */
public class LongWritable implements Writable  {

	private static final long serialVersionUID = 1L;
	
	long value;
	
	public LongWritable(){
		value = 0;
	}
	
	public LongWritable(long _val){
		value = _val;
	}
	
	public long get(){
		return this.value;
	}
	
	@Override
	public String toString() {
		return value+"";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	}
}
