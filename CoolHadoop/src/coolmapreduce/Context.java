package coolmapreduce;

public class Context {
	/**
	 * .write from Mapper
	 * For key value pairs from the mapper class
	 * - write the values into files marked by keys
	 * 		./output/<JobName>/mapper/<key>/values.txt
	 * 
	 * */
	
	
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
	 
	
}
