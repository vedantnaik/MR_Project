package io;
import java.io.Serializable;

public class DoubleWritable implements Serializable  {

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
}
