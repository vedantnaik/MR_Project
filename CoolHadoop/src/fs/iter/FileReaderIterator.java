package fs.iter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import fs.FileSys;
/**
 * 
 * @author Vedant_Naik, Vaibhav_Tyagi
 *
 */
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
		T objToReturn = this.cachedObj;
		this.cachedObj = null;
		return objToReturn;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException("Remove not supported by us!"); }

	@Override
	public Iterator<T> iterator() {
		try {
			return new FileReaderIterator<T>(this.fileToRead);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public void close() {
		try {
			this.ois.close();
		} catch (IOException e) {
			System.out.println("OIS already closed");			
		}		
	}
}
