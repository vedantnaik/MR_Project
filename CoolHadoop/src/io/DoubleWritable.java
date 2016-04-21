package io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class DoubleWritable implements Serializable, Writable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	double value;
	
	
	public DoubleWritable(){
		value = 0;
	}
	
	public DoubleWritable(double _val){
		value = _val;
	}

	
	public double get(){
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
