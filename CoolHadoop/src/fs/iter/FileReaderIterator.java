package fs.iter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import fs.FileSys;

public class FileReaderIterator implements Iterable<Object>, Iterator<Object>{

	ObjectInputStream ois;
	File fileToRead;
	Object cachedObj;
	
	public FileReaderIterator(File fileToRead) throws IOException {
		this.fileToRead = fileToRead;
		FileInputStream fileStream = new FileInputStream(fileToRead);
		this.ois = FileSys.getOIS(fileStream);
	}

	@Override
	public boolean hasNext() {
		try {
			this.cachedObj = ois.readObject();
			return true;
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	@Override
	public Object next() {
		// TODO: make copy constructor
		// 		find a better approach
		Object objToReturn = this.cachedObj;//.toString();
		this.cachedObj = null;
		return objToReturn;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException("Remove not supported by us!"); }

	@Override
	public Iterator<Object> iterator() {
		FileInputStream fileStream;
		try {
			return new FileReaderIterator(this.fileToRead);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}


	
}
