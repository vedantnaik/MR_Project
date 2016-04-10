package coolmapreduce;


import fs.FileSys;
import io.Text;
import utils.Constants;

public class Context {

	
	
	/**
	 * TODO: 
	 * Context should write based on the 
	 * map's output key and map's output value
	 */
	
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
	
	/**
	 * .write from Reducer
	 * - write the values into files in tab separated format
	 * 		KEY+"\t"+VALUE
	 * 		./output/<JobName>/reducer/parts/part-000<serverNumber>
	 * 
	 * */
	 
	private Job currentJob;		// MR Job for which this class is instantiated
	private String writePhase;	// The phase in which this class is instantiated
								// if Constants.CTX_MAP_PHASE : write to temp output
								// if Constants.CTX_RED_PHASE : write to final output
	private int localServerNumber;
	
	public Context(Job _currentJob, String _writePhase, int _localServerNumber){
		this.currentJob = _currentJob;
		this.writePhase = _writePhase;
		this.localServerNumber = _localServerNumber;
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
	public void write(Text keyToWrite, Text valueToWrite){
		if(this.writePhase.equalsIgnoreCase(Constants.CTX_MAP_PHASE)){
			writeToMapperOutput(keyToWrite, valueToWrite);
		} else {
			writeToReducerOutput(keyToWrite, valueToWrite);
		}
	}

	/**
	 * Write the output to the temporary location on the disk
	 * in RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE = "./output/<JOBNAME>/mapper/<KEY>/values<SERVERNUMBER>.txt";
	 * */
	private void writeToMapperOutput(Text keyToWrite, Text valueToWrite) {
		FileSys.writeMapperValueToKeyFolder(keyToWrite, valueToWrite, this.currentJob.getJobName(), this.localServerNumber);
	}


	/**
	 * Write final output key value pair
	 * 
	 * Key and Value will always be objects of Text
	 * */
	private void writeToReducerOutput(Text keyToWrite, Text valueToWrite) {
		FileSys.writeReducerOutputKeyValue(keyToWrite, valueToWrite, this.currentJob.getJobName());
	}
}
