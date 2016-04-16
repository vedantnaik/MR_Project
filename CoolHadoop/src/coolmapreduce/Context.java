package coolmapreduce;


import java.util.HashMap;

import fs.FileSys;
import io.Text;
import utils.Constants;

public class Context {
	
	/**
	 * TODO:
	 * if we want super.setup and
	 * super.cleanup we'll need to either stub them 
	 * and set the context there, else pass from Job
	 * to context(in MapperHandler) and then call 
	 * super.setup to set it there.
	 * 
	 *  OR
	 *  
	 *  stub them and not implement since, its 
	 *  not required to implement  
	 */
	
	
	/***********************************************************
	 * MKM mapper key maps
	 ***********************************************************
	 *
	 *	To deal with mapper keys that may have any form of special characters
	 *	or ones which are not really strings:
	 *
	 *	Solution: 
	 *	-	We use hashcodes in place of key.toString() while making the
	 *	key folders
	 *	-	Since we will need the actual value of the key to make the reduce call
	 *	we are going to maintain all the key.hashcodes : key.actualvalue in a map
	 *	called the MapperKeysMap (MKMs)
	 *
	 * 
	 * */
	
	
	private Job currentJob;		// MR Job for which this class is instantiated
	private String writePhase;	// The phase in which this class is instantiated
								// if Constants.CTX_MAP_PHASE : write to temp output
								// if Constants.CTX_RED_PHASE : write to final output
	private int localServerNumber;
	
	private HashMap<Integer, Object> mapperKeysMap;
	
	
	public Context(Job _currentJob, String _writePhase, int _localServerNumber){
		this.currentJob = _currentJob;
		this.writePhase = _writePhase;
		this.localServerNumber = _localServerNumber;
		
		if(_writePhase.equalsIgnoreCase(Constants.CTX_MAP_PHASE)){
			this.mapperKeysMap = new HashMap<Integer, Object>(); 
		}
	}
	
	/**
	 * Called from the user's map reducer program jar
	 * in the map() or reduce() function [or during cleanup]
	 * 
	 * EFFECT: based on what phase the program is in right now
	 * the method writes the output to 
	 * disk (temp | map output)		OR
	 * output S3 (final | reducer output)
	 * */
	public void write(Text keyToWrite, Object valueToWrite){
		if(this.writePhase.equalsIgnoreCase(Constants.CTX_MAP_PHASE)){
			writeToMapperOutput(keyToWrite, valueToWrite);
		} else {
			writeToReducerOutput(keyToWrite, valueToWrite);
		}
	}

	/**
	 * Write the output to the temporary location on the disk
	 * in RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE = "./output/<JOBNAME>/mapper/<KEY>/values<SERVERNUMBER>.txt";
	 * 
	 * Before replace the key with a hashcode of that key. Maintian the Hashcode in the mapperKeyMap
	 * */
	private void writeToMapperOutput(Text keyToWrite, Object valueToWrite) {
		
		this.mapperKeysMap.put(keyToWrite.hashCode(), keyToWrite);
		
		FileSys.writeMapperValueToKeyFolder(keyToWrite.hashCode(), valueToWrite, this.currentJob.getJobName(), this.localServerNumber);
	}


	/**
	 * Write final output key value pair
	 * 
	 * Key and Value will always be objects of Text
	 * 
	 ********************************************************************** 
	 * .write from Reducer
	 * - write the values into files in tab separated format
	 * 		KEY+"\t"+VALUE
	 * 		./output/<JobName>/reducer/parts/part-000<serverNumber>
	 ********************************************************************** 
	 * */
	private void writeToReducerOutput(Text keyToWrite, Object valueToWrite) {
		FileSys.writeReducerOutputKeyValue(keyToWrite, valueToWrite, this.currentJob.getJobName());
	}

	// GETTER SETTER

	public HashMap<Integer, Object> getMapperKeysMap() {
		return mapperKeysMap;
	}

	public void setMapperKeysMap(HashMap<Integer, Object> mapperKeysMap) {
		this.mapperKeysMap = mapperKeysMap;
	}
	
}
