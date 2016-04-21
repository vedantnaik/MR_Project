package fs.shuffler;

import java.util.HashMap;

import coolmapreduce.Configuration;
import coolmapreduce.Job;

public class LoadDistributorTester {

	
	public static void main(String[] args) {
//		test_genListForMapperOutputKeys();

		test_moveValuesFilesToReducerInputLocations();
	}
	




	private static void test_moveValuesFilesToReducerInputLocations() {
		
		String jobName = "testJob";
		String key = "key1";
		int localServerNumber = 1;
		
		Job currentJob = Job.getInstance(null);
		currentJob.setJobName(jobName);
		
		HashMap<String, Object> smap = new HashMap<String, Object>();
		smap.put(key.hashCode()+"", localServerNumber);
		
		LoadDistributor.moveValuesFilesToReducerInputLocations(
				smap, 
				localServerNumber, 
				currentJob);
		
		
		
	}





	private static void test_genListForMapperOutputKeys() {
		
		String jobName = "testJob";
		
		HashMap<String, Integer> mapperOutputKeySizerMap = LoadDistributor.genListForMapperOutputKeys(jobName);
		
		for (String key : mapperOutputKeySizerMap.keySet()){
			System.out.println(key + " : " + mapperOutputKeySizerMap.get(key));
		}
	}
}
