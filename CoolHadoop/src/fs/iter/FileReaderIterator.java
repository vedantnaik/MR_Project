package fs.iter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

import fs.FileSys;
import io.Text;

public class FileReaderIterator implements Iterator<Text>{

	ObjectInputStream ois;
	
	public FileReaderIterator(File fileToRead) throws IOException {
		FileInputStream fileStream = new FileInputStream(fileToRead);
		this.ois = FileSys.getOIS(fileStream);
	}

	@Override
	public boolean hasNext() {
		try {
			System.out.println(ois.available() + " ");
			return ois.available() > 0;
		} catch (IOException e) {
			System.out.println("EXCEPTION");
			return false;
		}
	}

	@Override
	public Text next() {
		try {
			return new Text(ois.readObject().toString());
		} catch (ClassNotFoundException e) {
//			// TODO FIX THIS
//			e.printStackTrace();
		} catch (IOException e) {
//			// TODO FIX THIS
//			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void remove() { throw new UnsupportedOperationException("Remove not supported by us!"); }


	
}
