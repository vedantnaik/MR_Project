package io;
import java.io.Serializable;


public class IntWritable implements Serializable  {

	/**
	 * 
	 */
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
		// TODO Auto-generated method stub
		return value+"";
	}
}
