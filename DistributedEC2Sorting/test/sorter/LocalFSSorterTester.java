package sorter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import datafile.DataRecord;
import utils.FileSystem;
import utils.S3FileReader;

public class LocalFSSorterTester {

	
	public static void main(String[] args) throws IOException {
		
		ArrayList<DataRecord> drlist = FileSystem.readInputDataRecordsFromInputBucket("cs6240sp16", "climate/199703hourly.txt.gz");
		
		ArrayList<Double> pivs = new ArrayList<Double>();
		pivs.add(30.9);
		pivs.add(34.1);
		pivs.add(57.9);
		pivs.add(64.9);
		pivs.add(75.6);
		pivs.add(87.8);
		
		
		ArrayList<Integer> indexPartList = new ArrayList<Integer>();
		Collections.sort(drlist);
		System.out.println("Size " + drlist.size());
		
		int pivCounter = 0;
		
		int pivIndex = 0;
		Double pivToConsider = pivs.get(pivIndex);
		int drIndexCounter = 0;

		for(DataRecord dr : drlist){
			
			if(dr.getSortValue() > pivToConsider){
				indexPartList.add(drIndexCounter);
				if(pivIndex == pivs.size()-1){
					indexPartList.add(drlist.size()-1);
					break;
				}
				pivToConsider = pivs.get(++pivIndex);
				System.out.println("consider " + pivToConsider);
			}
			drIndexCounter++;
		}
		
		
		
		System.out.println("\n\n parts : ");
		
		HashMap<Integer, ArrayList<DataRecord>> partsMap = new HashMap<Integer, ArrayList<DataRecord>>();
		
		int serverNum = 0;
		int prev = 0;
		for(Integer dr : indexPartList){
			System.out.println(serverNum + " : parts : " + prev + " to " +  dr);
			
			partsMap.put(serverNum, new ArrayList<DataRecord>(drlist.subList(prev, dr)));
			
			serverNum++;
			prev = dr;
		}
		
		
		
//		for(DataRecord sdr : partsMap.get(2)){
//			System.out.println(sdr.getFileName() + "");
//		}
		
	}
	
}
