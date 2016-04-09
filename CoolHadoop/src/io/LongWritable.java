package io;
import java.io.Serializable;

public class LongWritable implements Serializable  {

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
}
