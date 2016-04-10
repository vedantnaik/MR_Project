package fs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import coolmapreduce.Job;
import fs.iter.FileReaderIterator;
import io.Text;

public class FileSysTester {

	public static void main(String[] args) {
		
//		test_writeMapperValueToKeyFolder();
		
		
		Job job = Job.getInstance(null);
		job.setJobName("testJob");
//		FileSys.moveMapperTempFilesToLocalReducer("key1", 2, job);
		
//		test_combineReducerInputFiles();
		
		test_readMapperOutputForKey();
		
	}
	
	
	private static void test_readMapperOutputForKey() {
		
		Text key = new Text("key1");
		String jobName = "testJob";
		
		try {
			Iterable iter = (Iterable) FileSys.readMapperOutputForKey(key, jobName, 0);
			
			for(Object v : iter){
				System.out.println(v.toString());
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


	private static void test_writeMapperValueToKeyFolder(){
		Text key = new Text("key1");
		String jobName = "testJob";
		int localServerNumber = 2;
		
		
		String valServerId = "_"+localServerNumber;
		ArrayList<Text> vals = new ArrayList<Text>();
		vals.add(new Text("value0"+valServerId));
		vals.add(new Text("value1"+valServerId));
		vals.add(new Text("value2"+valServerId));
		vals.add(new Text("value3"+valServerId));
		vals.add(new Text("value4"+valServerId));
		vals.add(new Text("value5"+valServerId));
		vals.add(new Text("value6"+valServerId));
		vals.add(new Text("value7"+valServerId));
		vals.add(new Text("value8"+valServerId));

		for(Text v : vals){
			FileSys.writeMapperValueToKeyFolder(key, v, jobName, localServerNumber);
			
		}
		
		
	}
	
	
	private static void test_combineReducerInputFiles(){
		Text key = new Text("key1");
		String jobName = "testJob";
		int localServerNumber = 3;
		
		try {
			FileSys.combineReducerInputFiles(key, jobName, localServerNumber);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
	
	
}
