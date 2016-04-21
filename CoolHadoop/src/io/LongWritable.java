package io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class LongWritable implements Writable  {

	/**
	 * 
	 */
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
		// TODO Auto-generated method stub
		return value+"";
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
