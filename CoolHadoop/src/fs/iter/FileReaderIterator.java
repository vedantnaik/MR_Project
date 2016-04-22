package fs.iter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import fs.FileSys;

public class FileReaderIterator<T> implements Iterable<T>, Iterator<T>{

	ObjectInputStream ois;
	File fileToRead;
	T cachedObj;
	
	public FileReaderIterator(File fileToRead) throws IOException {
		this.fileToRead = fileToRead;
		FileInputStream fileStream = new FileInputStream(fileToRead);
		this.ois = FileSys.getOIS(fileStream);
	}

	@Override
	public boolean hasNext() {
		try {
			this.cachedObj = (T) ois.readObject();
			return true;
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	@Override
	public T next() {
		// TODO: make copy constructor
		// 		find a better approach
		T objToReturn = this.cachedObj;//.toString();
		this.cachedObj = null;
		return objToReturn;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException("Remove not supported by us!"); }

	@Override
	public Iterator<T> iterator() {
//		FileInputStream fileStream;
		try {
			return new FileReaderIterator<T>(this.fileToRead);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}


	
}
