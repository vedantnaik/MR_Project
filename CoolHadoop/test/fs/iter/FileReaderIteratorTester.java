package fs.iter;

import java.io.File;
import java.io.IOException;

import fs.iter.FileReaderIterator;
import io.Text;

public class FileReaderIteratorTester {

	public static void main(String[] args) throws IOException {
		
		Text key = new Text("key1");
		String jobName = "testJob";
		String fileToRead = "./input/"+jobName+"/reducer/"+key.toString()+"/values.txt";
		
		
		FileReaderIterator iter = new FileReaderIterator(new File(fileToRead));
		
		Text readObj;
		
		while(null != (readObj = iter.next())){
			System.out.println(readObj.toString());
		}

	}
	
}
