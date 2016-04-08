package fs.iter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;
import java.lang.Iterable;

import fs.FileSys;
import io.Text;

public class FileReaderIterator implements Iterable<Text>, Iterator<Text>{

	ObjectInputStream ois;
	File fileToRead;
	Text cachedObj;
	
	public FileReaderIterator(File fileToRead) throws IOException {
		this.fileToRead = fileToRead;
		FileInputStream fileStream = new FileInputStream(fileToRead);
		this.ois = FileSys.getOIS(fileStream);
	}

	@Override
	public boolean hasNext() {
		try {
			this.cachedObj = new Text(ois.readObject().toString());
			return true;
		} catch (IOException e) {
			return false;
		} catch (ClassNotFoundException e) {
			return false;
		}
	}

	@Override
	public Text next() {
		// TODO: make copy constructor
		// 		find a better approach
		Text objToReturn = new Text(this.cachedObj.toString());
		this.cachedObj = null;
		return objToReturn;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException("Remove not supported by us!"); }

	@Override
	public Iterator<Text> iterator() {
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
