package coolmapreduce;

public class ReducerHandeler {

	
	/**
	 * TODO: Reducer Handler Req.
	 * 
	 * APPEND ALL INPUT FILES WHILE SHUFFLING TO ONE FILE FOR THAT KEY
	 * 
	 * 
	 * 1. Get signal from master node to start reduce phase
	 * 		Need location of temp output by mapper nodes
	 * 2. Move my temp output files from all mappers for a job to my instance
	 * 				./output/<JobName>/mapper/<key>/values.txt
	 * 3. For each key make a reduce call
	 * 3. Combine all mapper output files for a key such that,
	 * 		for each key : list of iterables for its values are given
	 * 
	 * */
	
	
	public void moveAndMergeTempOutput(){
		
		
		
	}
	
}
