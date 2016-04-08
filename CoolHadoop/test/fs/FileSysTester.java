package fs;

import java.util.ArrayList;

import io.Text;

public class FileSysTester {

	public static void main(String[] args) {
		
		test_writeMapperValueToKeyFolder();
		
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
		
		
		
		ArrayList<Text> vals = new ArrayList<Text>();
		vals.add(new Text("value"));
		vals.add(new Text("value1"));
		vals.add(new Text("value2"));
		vals.add(new Text("value3"));
		vals.add(new Text("value4"));
		vals.add(new Text("value5"));
		vals.add(new Text("value6"));
		vals.add(new Text("value7"));
		vals.add(new Text("value8"));

		for(Text v : vals){
			FileSys.writeMapperValueToKeyFolder(key, v, jobName);
			
		}
		
		
	}
	
	
	
	
	
}
