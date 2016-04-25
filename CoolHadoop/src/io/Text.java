package io;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;


public class Text implements Writable{

	private static final long serialVersionUID = 1L;
	String text;
	
	public Text(){
		text = new String();
	}
	
	public Text(String _text){
		text = new String(_text);
	}
	
	public Text(Text _text){
		text = new String(_text.text);
	}
	
	@Override
	public String toString() {
		return this.text;
	}
	
	public void set(String _text){
		text = _text;
	}
	
	@Override
	public int hashCode() {
		return text.hashCode();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
	}


	@Override
	public void write(DataOutput arg0) throws IOException {
	}
		
}
