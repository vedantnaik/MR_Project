package io;
import java.io.Serializable;

public class BooleanWritable implements Serializable  {

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
	
	
}
