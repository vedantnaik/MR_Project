package io;
import java.io.Serializable;


public class Text implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String text;
	
	public Text(){
		text = new String();
	}

	
	public Text(String _text){
		text = new String(_text);
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.text;
	}
	
	public void set(String _text){
		text = _text;
	}
	
	@Override
	public int hashCode() {
		return text.hashCode();
	}
	
//	@Override
//	public boolean equals(Object obj) {
//		// TODO Auto-generated method stub
//		return text.equals((Text) obj);
//	}
	
}
