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
public class IntWritable implements Writable  {

	private static final long serialVersionUID = 1L;
	
	int value;
	
	public IntWritable(){
		value = 0;
	}
	
	public IntWritable(int _val){
		value = _val;
	}
	
	public int get(){
		return this.value;
	}
	
	public void set(int _value){
		value = _value;
	}
	
	@Override
	public String toString() {
		return value+"";
	}
	
	@Override
	public int hashCode() {
		return value;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
	}
}
