package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
/**
 * @author Vedant_Naik, Vaibhav_Tyagi, Dixit_Patel, Rohan_Joshi
 *
 */
public interface Writable extends Serializable {
	
	public void readFields(DataInput arg0) throws IOException;
	
	public void write(DataOutput arg0) throws IOException;

}
