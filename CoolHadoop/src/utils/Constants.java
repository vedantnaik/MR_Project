package utils;
/**
 * 
 * @author Vedant_Naik, Vaibhav_Tyagi, Dixit_Patel, Rohan_Joshi
 *
 */
public class Constants {
	
	public static final String PROJECT_HOME = "/home/ubuntu/Project/";

	public static final String PUBLIC_DNS_FILE = "publicDnsFile.txt";
	
	// confMap keys
	public static final String CTX_INPUT_PATH_KEY = "INPUT_PATH";
	public static final String CTX_OUTPUT_PATH_KEY = "OUTPUT_PATH";
	
	public static final String INPUT_BUCKET_NAME = "INPUT_BUCKET_NAME";
	public static final String OUTPUT_BUCKET_NAME = "OUTPUT_BUCKET_NAME";

	public static final String INPUT_FOLDER = "INPUT_FOLDER";
	public static final String OUTPUT_FOLDER = "OUTPUT_FOLDER";
	
	public static final String S3PREFIX = "s3://";
	public static Integer BUCKET_INT = 0;
	public static Integer OBJECT_INT = 1;
	
	
	// to read master server's dns from serverIPaddrMap 
	public static final String MASTER_SERVER_IP_KEY = "MASTER_SERVER_DNS";
	
	// File system
	
	// --	Mapper
	public static final String UNIX_FILE_SEPARATOR = "/";
	
	public static final String RELATIVE_MAPPER_CONTEXT_OUTPUT_FOLDER = "./output/<JOBNAME>/mapper/";
	
	public static final String RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE = "./output/<JOBNAME>/mapper/<KEY>/values<SERVERNUMBER>.txt";
	public static final String ABSOLUTE_MAPPER_CONTEXT_OUTPUT_FILE = "~/Project/output/<JOBNAME>/mapper/<KEY>/values<SERVERNUMBER>.txt";
	
	// MAPPER KEY MAP
	// on main server
	public static final String MASTER_MAPPER_KEY_MAPS_FOLDER_LOCAL = "output/<JOBNAME>/MasterMKMs/";
	
	public static final String MASTER_MAPPER_KEY_MAPS_FOLDER = PROJECT_HOME+"/output/<JOBNAME>/MasterMKMs/";
	public static final String ABSOLUTE_MASTER_MAPPER_KEY_MAPS_FOLDER = "~/Project/output/<JOBNAME>/MasterMKMs/";
	
	public static final String ABSOLUTE_MASTER_MKM_PATH_FOLDER = PROJECT_HOME + "/output/<JOBNAME>/MKMs/";
	public static final String RELATIVE_MAPPER_KEY_MAPS_FOLDER = "./output/<JOBNAME>/MKMs/";
	
	public static final String MKM_FILE_NAME = "mkmonserver<SERVERNUMBER>";
	public static final String BROADCAST_MKM_MAP = "mkmbroadcastmaster";
	public static final String BROADCAST_KEY_SERVER_MAP = "broadcastkeyservermaster";
	
	public static final String ABSOLUTE_OUTPUT_LOCATION = PROJECT_HOME + "/output/";
	public static final String ABSOLUTE_JOB_FOLDER = ABSOLUTE_OUTPUT_LOCATION + "/<JOBNAME>/";
	public static final String ABSOLUTE_MAPPER_FOLDER = ABSOLUTE_JOB_FOLDER + "/mapper/";
	
	// --	Reducer
	public static final String RELATIVE_REDUCER_INPUT_FOLDER = "./input/<JOBNAME>/reducer/<KEY>/";
	public static final String ABS_REDUCER_INPUT_FOLDER = PROJECT_HOME + "input/<JOBNAME>/reducer/<KEY>/";
	public static final String ABS_REDUCER_JUST_INPUT_FOLDER = PROJECT_HOME + "input/<JOBNAME>/reducer/";
	
	public static final String ABSOLUTE_REDUCER_INPUT_FOLDER = "~/Project/input/<JOBNAME>/reducer/<KEY>/";
	
	public static final String RELATIVE_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt";
	public static final String ABSOLUTE_REDUCER_INPUT_FILE = "Project/input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt";
	public static final String ABS_REDUCER_INPUT_FILE = PROJECT_HOME + "input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt";
	
	public static final String RELATIVE_COMBINED_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values.txt";
	
	public static final String RELATIVE_REDUCER_OUTPUT_FILE = "./output/<JOBNAME>/reducer/part-XXXXX";
	public static final String RELATIVE_REDUCER_OUTPUT_FOLDER = "./output/<JOBNAME>/reducer";
	
	// Network communication
	// NOTE: Changing from ./configFiles to ./credentials
	public static final String PEM_FILE_PATH = "~/Project/credentials/MyKeyPair.pem";

	public static final String EC2_USERNAME = "ubuntu";
	public static final int SSH_PORT = 22;
	public static final String READJOB = "READ_JOB";
	public static final String JOBREAD = "JOB_READ";
	
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
	public static final String SHUFFLEANDSORT = "SHUFFLEANDSORT";
	public static final String KILL = "KILL";
	public static final String SHUFFLEFINISH = "SHUFFLEFINISH";	
	public static final String REDUCEFINISH = "REDUCEFINISH";
	public static final String JOBEXTN = ".jobname";
	

	public static final String START_MAP = "START_MAP";	
	public static final String FILES_READ = "FILES_READ";

	// MAP REDUCE phase identifier for Context.write()
	public static final String CTX_MAP_PHASE = "CTX_MAP_PHASE";
	public static final String CTX_RED_PHASE = "CTX_RED_PHASE";	
}
