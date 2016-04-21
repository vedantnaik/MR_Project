package io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public interface Writable extends Serializable {

	
	public void readFields(DataInput arg0) throws IOException;
	
	public void write(DataOutput arg0) throws IOException;

}
