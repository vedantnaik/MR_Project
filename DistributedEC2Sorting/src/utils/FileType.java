package utils;

import java.io.File;

public class FileType implements Comparable<FileType>{
	File file;
//	String fileName;
	double size;
	
	public FileType() {
		// TODO Auto-generated constructor stub
	}
	
	public FileType(File _file){
		file = _file;
		size = ((file.length() * 1.0) / (  1 << 20 ));
	}
	
	public FileType(File _parent, String child){
		file = new File(_parent, child);
		size = ((file.length() * 1.0) / (  1 << 20 ));
	}
	
	@Override
	public String toString() {
		return "(" + file.getName() + ", " + size + ")";
	}

	@Override
	public int compareTo(FileType f) {
		return (int) (size - f.size);
	}
	
	@Override
	public boolean equals(Object obj) {
		return file.getName() == ((FileType)obj).file.getName();
	}
	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return file.getName().hashCode();
	}
	
	
}
