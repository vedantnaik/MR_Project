package fs.shuffler;

import java.io.File;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import coolmapreduce.Job;
import fs.FileSys;
import static java.nio.file.StandardCopyOption.*;
import utils.Constants;

public class LoadDistributor {

	/**
	 * Each server
	 * - read list of keys and size from mapper output folder for a JOB
	 * 		input jobname
	 * 
	 * - HashMap of key,size
	 * - send this HashMap to server0
	 * 
	 * At server0:
	 * 		- distrib load
	 * 		- broadcast final decision in this format
	 * 		-	Map
	 * 			key	:	ServerNumber
	 * 			value:	List<Keys>
	 * 
	 * 
	 * At each server:
	 * 		- check what keys this server will handle
	 * 		- send key values to other servers as per the broadcast
	 * */

	
	/**
	 * Each server
	 * - read list of keys and size from mapper output folder for a JOB
	 * 		input jobname
	 * 
	 * - HashMap of key,size
	 * - send this HashMap to server0
	 *
	 * USAGE:
	 * At each EC2
	 * After map phase is complete
	 * before sending keys to server0
	 * 
	 * */
	
	public static HashMap<String, Integer> genListForMapperOutputKeys(String jobName){
		
		HashMap<String, Integer> mapperOutputKeySizerMap = new HashMap<String, Integer>();
		
		String mapperOutputFolderStr = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FOLDER
												.replace("<JOBNAME>", jobName);
		File mapperOutputFolder = new File(mapperOutputFolderStr);
		
		for(String f : mapperOutputFolder.list()){
			File fSeen = new File(mapperOutputFolder+"/"+f);
			if(fSeen.isDirectory()){
				mapperOutputKeySizerMap.put(f, folderSize(fSeen));
			}
		}
		return mapperOutputKeySizerMap;
	}
	
	
	
	
	/**
	 * USAGE:
	 * At server0
	 * every time it gets a new set of key size maps
	 * */
	public static void mergeWithMainMapperOutputKeySizerMap(HashMap<String, Integer> mainServerMapperKeySizeMap, HashMap<String, Integer> newMap){
		for(String newKey : newMap.keySet()){
			if(mainServerMapperKeySizeMap.containsKey(newKey)){
				int sizeSoFar = mainServerMapperKeySizeMap.get(newKey) + newMap.get(newKey);
				mainServerMapperKeySizeMap.put(newKey, sizeSoFar);
			} else {
				mainServerMapperKeySizeMap.put(newKey, newMap.get(newKey));				
			}
		}
	}
	
	
	
	
	
	/**
	 * @param
	 * serverMapperOutputKeySizerMap
	 * 		format
	 * 		{Key	:	Size}
	 * 
	 * parts
	 * 		number of EC2 servers
	 * 
	 * Distribute load between servers
	 * @return
	 * map {Key : ServerNumber}
	 * 
	 * USAGE:
	 * at server0
	 * before shuffle stage
	 * 
	 * */
	
	public static Map<String, Object> getLoadDistribBroadcast(Map<String, Object> serverMapperOutputKeySizerMap, int parts){
		
		Map<String, Object> broadcastMap = new HashMap<String, Object>();
		
//		System.out.println("entry set " + serverMapperOutputKeySizerMap.entrySet());
		
		// Sort map based on the key's sizes
		ArrayList<Entry<String, Object>> sortedMapperKeySizeList = new ArrayList<Entry<String, Object>>(serverMapperOutputKeySizerMap.entrySet());
				
		// assign a server to each key (used to tell servers which keys they will handle)
		int spinner = 0;
//		System.out.println("broad cast " + sortedMapperKeySizeList);
		for(Entry<String, Object> e : sortedMapperKeySizeList){
			String mapperKey = e.getKey();
			if(spinner == parts) {spinner = 0;}
			broadcastMap.put(mapperKey, spinner);
			spinner++;
		}
		return broadcastMap;
	}
	
	
	
	/**
	 * 
	 * USAGE:
	 * at each server
	 * after received broadcast map
	 * */
	// NOTE: Changing mapkey from string to int, for both moveMapperTempFiles*
	public static void moveValuesFilesToReducerInputLocations(Map<String, Object> broadcastMap, int localServerNumber, Job currentJob){
		
//		String jobName = currentJob.getJobName();
		
		for (String mapKey : broadcastMap.keySet()){
			int serverToMoveTo = (int)broadcastMap.get(mapKey);
			
			if(serverToMoveTo == localServerNumber){
				
				FileSys.moveMapperTempFilesToLocalReducer(mapKey, localServerNumber, currentJob);
				
			} else {
				// scp to another
				try {
					FileSys.moveMapperTempFilesToRemoteReducers(localServerNumber, mapKey, serverToMoveTo, currentJob);
				} catch (SocketException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (JSchException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SftpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
	
	
	
	
	
	// HELPERS
	
	// Reference : http://stackoverflow.com/questions/2149785/get-size-of-folder-or-file
	public static int folderSize(File directory) {
	    int length = 0;
	    for (File file : directory.listFiles()) {
	        if (file.isFile())
	            length += file.length();
	        else
	            length += folderSize(file);
	    }
	    return length;
	}




	public static void makeAllKeyFolderLocations(
			Map<String, Object> broadcastMap, String jobname) {
		try {

		String reducerInputLocation = Constants.ABS_REDUCER_JUST_INPUT_FOLDER
				.replace("<JOBNAME>", jobname);
		
		System.out.println("recursively deleting " + reducerInputLocation);
		FileUtils.deleteDirectory(new File(reducerInputLocation));
		
		System.out.println("recreating it ");
		File reducerInputLocationFolder = new File(reducerInputLocation);
		reducerInputLocationFolder.mkdir();
		
		for (String keys : broadcastMap.keySet()){
			String hashcode = (String) keys;
			
			String reducerInputLocationKeyFolder = reducerInputLocation + hashcode
					+ Constants.UNIX_FILE_SEPARATOR;
			System.out.println("creating reducer input folder with key "
					+ reducerInputLocationKeyFolder);
			File keyFolder = new File(reducerInputLocationKeyFolder);
			keyFolder.mkdir();
		}
		
		
		} catch (IOException e) {
			System.out.println("Error in making folders for all keys");
			e.printStackTrace();
		}
		
	}	
}













