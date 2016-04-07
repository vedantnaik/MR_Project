package fileprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;

import datafile.DataRecord;
import sorter.LocalFSSorter;
import utils.FileSystem;

public class SortMyPartsTester {
	
	static boolean firstReadFlag = false;
	
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		
		
		ArrayList<DataRecord> serverDataRecordsCache = new ArrayList<DataRecord>(); 
	
		System.out.println("Begin sorting complete serverDataRecordsCache..." + serverDataRecordsCache.size());
		// 1. Read this server partitions written from previous step to get complete record list
		
		
		readMyParts(7);
		readMyParts(7);
		
//		BufferedReader printer = new BufferedReader(new FileReader(new File("MYPARTS_SORTED")));
//		String line;
//		while(null != (line = printer.readLine())){
//			System.out.println(line);
//		}
//		printer.close();
		
		// 2. Sort complete record list
//		Collections.sort(serverDataRecordsCache);
		System.out.println("Complete partition for server sorted.");
		
	}
	
	
	
	
	public static void readMyParts(int serverNumber) throws IOException, ClassNotFoundException{
		
		
		File folderIn = new File("myTestParts");

		System.out.println("Folder In : " + folderIn);

		
		if(null != folderIn){
			System.out.println(folderIn.getName());
			for(File partFile : folderIn.listFiles()){
				
				if(partFile.getName().contains("p"+serverNumber)){
					System.out.println("Server "+serverNumber + " reading at " + partFile.getName());
					FileInputStream fileStream = new FileInputStream(folderIn+"/"+partFile.getName());
					ObjectInputStream ois = new ObjectInputStream(fileStream);
					
					ArrayList<DataRecord> readList = (ArrayList<DataRecord>) ois.readObject();
					Collections.sort(readList);
					System.out.println("Sizes to add : " + readList.size());
					
					if(!firstReadFlag){
						LocalFSSorter.writeToTempCache(readList, "MYPARTS_SORTED");
						firstReadFlag = true;
					} else {
						LocalFSSorter.mergeWithTempCache(readList, "MYPARTS_SORTED");
					}
					
					// trying reset
					ois.close();
					fileStream.close();
				}
			}
		}
	}
}
