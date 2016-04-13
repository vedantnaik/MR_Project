package mapper;	

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.Constants;
import coolmapreduce.Configuration;
import coolmapreduce.Job;
import coolmapreduce.MapperHandler;

public class MapperTester {
	
	public static String PATH = "C://Users//Dixit_Patel//Google Drive//Working on a dream//StartStudying//sem4//MapReduce//homeworks//hw8-Distributed Sorting//MR_Project//CoolHadoop//resources";
	
	public static List<String> getStupidFiles(){
		List<String> files = new ArrayList<>();
		files.add("alice.txt.gz");
		
		for(int i = 0 ; i< 3; i ++)
			files.add("alice.txt.gz");
		
		return files;
	}
	
	public static Map<Integer, List<String>> mimicMyParts(){
		Map<Integer, List<String>> parts = new HashMap<>();
		for(int i = 0 ; i < 3 ; i++)
			parts.put(i, getStupidFiles());
		
		return parts;
	}

	public static void main(String[] args) throws Exception {
		
		List<String> files = getStupidFiles();
		
		System.out.println("here " + mimicMyParts());
		
		Configuration conf = new Configuration();
		conf.set(Constants.INPUT_BUCKET_NAME, PATH);
		Job job = Job.getInstance(conf);
//		job.setMapperClass(TokenizerMapper.class);
		MapperHandler mh = new MapperHandler(files, job);
		mh.runMapperHandler();		
		
	}
}
