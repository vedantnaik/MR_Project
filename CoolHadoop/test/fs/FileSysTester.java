package fs;

import io.Text;

public class FileSysTester {

	public static void main(String[] args) {
		
//		test_writeMapperValueToKeyFolder();
		
//		test_readMapperOutputForKey();
		
	}
	
	
	private static void test_readMapperOutputForKey() {
		
		Text key = new Text("key1");
		String jobName = "testJob";
		
		FileSys.readMapperOutputForKey(key, jobName);
		
	}


	private static void test_writeMapperValueToKeyFolder(){
		Text key = new Text("key1");
		String jobName = "testJob";
		
		Text value = new Text("value");
		FileSys.writeMapperValueToKeyFolder(key, value, jobName);
		
		Text value2 = new Text("value2");
		FileSys.writeMapperValueToKeyFolder(key, value2, jobName);		
	}
	
	
	
	
	
}
