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
	 * .write from Reducer
	 * - write the values into files in tab separated format
	 * 		KEY+"\t"+VALUE
	 * 		./output/<JobName>/reducer/parts/part-000<serverNumber>
	 * 
	 * */
	 
	
}
