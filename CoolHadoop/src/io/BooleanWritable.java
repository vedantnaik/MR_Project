package io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class BooleanWritable implements Writable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	boolean value;
	
	
	public BooleanWritable(){
		
	}
	
	public BooleanWritable(boolean _val){
		value = _val;
	}

	
	public boolean get(){
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
//		value.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
//		value.write(arg0);
	}
	
	
}
