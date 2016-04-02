package sorter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ListIterator;

import datafile.DataRecord;
import utils.FileSystem;

//Checkpoint 3 : local FS and merger
public class LocalFSSorter {

	public static void main(String[] args) {
		
		ArrayList<DataRecord> drlist = FileSystem.readInputDataRecordsFromInputBucket("cs6240sp16", "climate/199703hourly.txt.gz");
		Collections.sort(drlist);
		writeToTempCache(drlist, "TEMPSORT1");
		
		
		
		ArrayList<DataRecord> drlist2 = FileSystem.readInputDataRecordsFromInputBucket("cs6240sp16", "climate/199702hourly.txt.gz");
		Collections.sort(drlist2);
		
		
		mergeWithTempCache(drlist2, "TEMPSORT1");
		
		
		
		ArrayList<DataRecord> merList = readFromTempCache("TEMPSORT1_merger");
		
		for(DataRecord m : merList){
			System.out.println(m.toString());
		}
		
		int size = drlist.size() + drlist2.size();
		System.out.println( drlist.size() + " " +  drlist2.size() );
		System.out.println( size + " " +  merList.size() );
	}
	
	
	public static int cacheFileSize(String tempCacheFileName){
		int cacheFileSize = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(tempCacheFileName)));
			String line;
			
			while(null != (line = reader.readLine())){
				cacheFileSize++;
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return cacheFileSize;
	}
	
	public static DataRecord getDataRecordAtIndexXinCache(String tempCacheFileName, int index){
		DataRecord drToReturn = null;
		int cacheFileIndex = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(tempCacheFileName)));
			String line;
			while(null != (line = reader.readLine())){
				if(cacheFileIndex == index){
					String[] fields = line.split(","); 
					drToReturn = getDataRecordFromWriteString(line);
					break;
				}
				cacheFileIndex++;
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		return drToReturn;
	}
	
	public static void writeToTempCache(ArrayList<DataRecord> recordsSoFar, String tempCacheFileName){
		
		try {
			BufferedWriter cache = new BufferedWriter(new FileWriter(new File(tempCacheFileName)));
		
			for(DataRecord drToWrite : recordsSoFar){
				cache.write(makeWriteString(drToWrite));
			}
		
			cache.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static ArrayList<DataRecord> readFromTempCache(String tempCacheFileName){
		
		ArrayList<DataRecord> cacheList = new ArrayList<DataRecord>();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(tempCacheFileName)));
			String line;
			
			while(null != (line = reader.readLine())){
//				System.out.println(line);
				cacheList.add(getDataRecordFromWriteString(line));
			}
			
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return cacheList;
		
	}
	
	// Checkpoint 3
	private static String makeWriteString(DataRecord drToWrite){
		return  drToWrite.getWban() + "," + drToWrite.getDate() 
		+ "," + drToWrite.getTime() + "," + drToWrite.getSortValue() + "\n";
	}
	
	private static DataRecord getDataRecordFromWriteString(String line){
		
		try{
			String[] fields = line.split(","); 
			return new DataRecord(Double.parseDouble(fields[3]), 
										fields[0], 
										Integer.parseInt(fields[1]), 
										fields[2]);
		} catch (Exception e) {
			System.out.println(line);
			System.exit(1);
			return null;
		}
	}
	
	public static void mergeWithTempCache(ArrayList<DataRecord> recordsToAdd, String tempCacheFileName){
		String mergerFileName = tempCacheFileName+"_merger";
		
		try {
			File mergerFile = new File(mergerFileName);
			BufferedWriter mergerWriter = new BufferedWriter(new FileWriter(mergerFile));
			
			File tempCacheFile = new File(tempCacheFileName);
			BufferedReader cacheReader = new BufferedReader(new FileReader(tempCacheFile));
			
			ListIterator<DataRecord> recordToAddIter = recordsToAdd.listIterator();
			
			String line;
			String[] fields;
			DataRecord drToAdd;
			DataRecord drCached;
			
			line = cacheReader.readLine();
			// double sortValue, String wban, int date, String time			
			drCached = getDataRecordFromWriteString(line);
			drToAdd = recordToAddIter.next();
			recordToAddIter.previous();
			while(recordToAddIter.hasNext() && line != null){
				if(drCached.compareTo(drToAdd) == -1){
					mergerWriter.write(makeWriteString(drCached));
					drCached = getDataRecordFromWriteString(line);
					line = cacheReader.readLine();
				} else {
					mergerWriter.write(makeWriteString(drToAdd));
					drToAdd = recordToAddIter.next();
				}
			}
			
			while(recordToAddIter.hasNext()){
				mergerWriter.write(makeWriteString(drToAdd));
				drToAdd = recordToAddIter.next();
			}
			
			while(line != null){
				mergerWriter.write(makeWriteString(drCached));
				fields = line.split(","); 
				drCached = getDataRecordFromWriteString(line);
				line = cacheReader.readLine();
			}
			
			cacheReader.close();
			tempCacheFile.delete();
			
			mergerWriter.close();
			File renameFileToCache = new File(tempCacheFileName);
			mergerFile.renameTo(renameFileToCache);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}



}
