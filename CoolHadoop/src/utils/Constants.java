package utils;

public class Constants {

	public static final String PUBLIC_DNS_FILE = "publicDnsFile.txt";
	
	// confMap keys
	public static final String CTX_INPUT_PATH_KEY = "INPUT_PATH";
	public static final String CTX_OUTPUT_PATH_KEY = "OUTPUT_PATH";
	public static final String INPUT_BUCKET_NAME = "INPUT_BUCKET_NAME";
	
	
	
	// File system
	
	// --	Mapper
	public static final String UNIX_FILE_SEPARATOR = "/";
	
	public static final String RELATIVE_MAPPER_CONTEXT_OUTPUT_FOLDER = "./output/<JOBNAME>/mapper/";
	
	public static final String RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE = "./output/<JOBNAME>/mapper/<KEY>/values<SERVERNUMBER>.txt";
	public static final String ABSOLUTE_MAPPER_CONTEXT_OUTPUT_FILE = "~/Project/output/<JOBNAME>/mapper/<KEY>/values<SERVERNUMBER>.txt";
	
	
	
	// --	Reducer
	public static final String RELATIVE_REDUCER_INPUT_FOLDER = "./input/<JOBNAME>/reducer/<KEY>/";
	public static final String ABSOLUTE_REDUCER_INPUT_FOLDER = "~/Project/input/<JOBNAME>/reducer/<KEY>/";
	
	public static final String RELATIVE_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt";
	public static final String ABSOLUTE_REDUCER_INPUT_FILE = "~/Project/input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt";
	
	public static final String RELATIVE_COMBINED_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values.txt";
	
	public static final String RELATIVE_REDUCER_OUTPUT_FILE = "./output/<JOBNAME>/reducer/part-XXXXX";
	
	// Network communication
	public static final String PEM_FILE_PATH = "./configFiles/MyKeyPair.pem";

	public static final String EC2_USERNAME = "ubuntu";
	public static final int SSH_PORT = 22;
	public static final String MAPFILES = "MAP_FILES";
	
	public static final String MAP = "MAP";	
	public static final String REDUCE = "REDUCE";	
	public static final String START = "START";
	public static final String END = "END";
	
	public static final String MAPFAILURE = "MAPFAILURE";
	public static final Object LOCAL = "LOCAL";
	
	
	
	// Phases
	
	public static final String OUR_INIT = "OUR_INIT";
	public static final String SETUP = "SETUP";
	public static final String CLEANUP = "CLEANUP";
	public static final String RUNNING = "RUNNING";

	public static final String MAP_FINISH = "MAP_FINISH";
	public static final String NEED_TO_STARTING_MAP = "NEED_TO_STARTING_MAP";

	public static final String START_MAP = "START_MAP";

	public static final String FILES_READ = "FILES_READ";

	// MAP REDUCE phase identifier for Context.write()
	
	public static final String CTX_MAP_PHASE = "CTX_MAP_PHASE";
	public static final String CTX_RED_PHASE = "CTX_RED_PHASE";
	
	/**
	 * Folders to make
	 * 
	 * ./output/
	 * ./output/<JOBNAME>
	 * ./output/<JOBNAME>/mapper
	 * ./output/<JOBNAME>/reducer
	 * 
	 * 
	 * 
	 * */
	
}
