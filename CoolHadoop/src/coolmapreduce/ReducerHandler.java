package coolmapreduce;

import fs.FileSys;
import io.Text;
import io.Writable;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import utils.Constants;

/**
 * 
 * @author Vedant_Naik, Dixit_Patel
 *
 */
public class ReducerHandler {

	
	/**
	 * Reducer Handler 
	 * 
	 * APPEND ALL INPUT FILES WHILE SHUFFLING TO ONE FILE FOR THAT KEY
	 * 
	 * 1. Get signal from master node to start reduce phase Need location of
	 * temp output by mapper nodes 
	 * 2. Move my temp output files from all mappers
	 * for a job to my instance ./output/<JobName>/mapper/<key>/values.txt 
	 * 3. For each key make a reduce call 3. Combine all mapper output files for a
	 * key such that, for each key : list of iterables for its values are given
	 * 
	 * */

	// Current Job variable
	Job currentJob;

	// String value indicating which phase the Job is in
	String phase = "";

	// Main broadcasted MKM which gives us info about original keys and their
	// hashed values used for folder names
	HashMap<String, Object> broadcastedMKM;

	// Class variable for Keeping track of Mapper class
	Class<?> classVariable = null;
	String classVariableName = "";

	// Actual instance of the Mapper
	Object objectInstance = null;

	// default values for each KI, VI, KO, VO
	// KI = keyIn, VI = ValueIn
	// KO = KeyOut, VO = ValueOut
	Class<?> keyInClass = Text.class;
	Class<?> valueInClass = Text.class;
	Class<?> keyOutClass = Text.class;
	Class<?> valueOutClass = Text.class;
	Class<?> contextClass = Context.class;

	int localServerNumber;

	// get context from job for now class variable
	Context contextVariable = null;

	// Assumes each MapperHandler has a list of files to work on
	public ReducerHandler() {
		currentJob = null;
	}

	public ReducerHandler(Job _currentJob,
			HashMap<String, Object> _broadcastedMKM, int _localServerNumber) {
		localServerNumber = _localServerNumber;
		currentJob = _currentJob;
		contextVariable = new Context(currentJob, Constants.CTX_RED_PHASE,
				localServerNumber);
		broadcastedMKM = _broadcastedMKM;
	}

	
	/**
	 * Given a master key server map, handle keys that are to be handled on this reducer instance
	 * by checking with the localServerNumber. If the key is to be handled on this server,
	 * read the values.txt file from the input folder and invoke the reduce call in Reducer
	 * */
	public void runReducerHandler(Map<String, Object> masterKeyServerMap) throws Exception {

		System.out.println("\tStarting Reducer for "
				+ currentJob.getReducerClass().toString());

		reducerHandlerInit();

		// calling map for each line

		// update accordingly
		phase = Constants.SETUP;
		reducerHandlerSetup();

		phase = Constants.RUNNING;
		
		int progressCounter = 0;
		
		for (String hashedKeyFolderName : broadcastedMKM.keySet()) {
			if ((int) masterKeyServerMap.get(hashedKeyFolderName) == localServerNumber) {
				System.out.println("processing key " + hashedKeyFolderName);

				String originalKey = (String) broadcastedMKM
						.get(hashedKeyFolderName);
				reducerHandlerForKey(new Text(originalKey), hashedKeyFolderName);
				phase = Constants.RUNNING + " " + (progressCounter * 100.0)
						/ broadcastedMKM.size();
			}
		}
		
		phase = Constants.CLEANUP;
		reducerHandlerCleanup();

		System.out.println("invoking move final reducer output to s3 bucket with bucket: " + 
				currentJob.getConf().get(Constants.OUTPUT_BUCKET_NAME) + " folder: " +  
				currentJob.getConf().get(Constants.OUTPUT_FOLDER));
		
		FileSys.moveFinalReducerOutputToS3Bucket(currentJob.getConf().get(Constants.OUTPUT_BUCKET_NAME), 
				currentJob.getConf().get(Constants.OUTPUT_FOLDER), currentJob.getJobName(), localServerNumber);
		phase = Constants.MAP_FINISH;
	}

	/**
	 * Invokes the setup method. If not overridden, the invoked method does noop
	 * */
	public void reducerHandlerSetup() throws Exception {
		// setup phase
		try {

			System.out.println("\tCalling setup::Reducer for " + classVariable);
			phase = "SETUP";
			Method setup = classVariable.getMethod("setup", contextClass);

			System.out.println("Invoking setup function");

			// give context variable
			setup.invoke(objectInstance, contextVariable);

		} catch (Exception e) {
			System.out.println("exception in init of setup");
			System.out.println("No setup found for Reducer");
		}

	}

	private void reducerHandlerInit() throws Exception {
		try {
			classVariableName = currentJob.getReducerClass().getName();
			classVariable = Class.forName(currentJob.getReducerClass()
					.getName());
			objectInstance = classVariable.newInstance();

			// setting anything except Text if MapOutputKeyClass()
			if (null != currentJob.getMapOutputKeyClass())
				keyInClass = currentJob.getMapOutputKeyClass();

			// setting anything except Text if MapOutputValueClass
			if (null != currentJob.getMapOutputValueClass())
				valueInClass = currentJob.getMapOutputValueClass();

			// setting anything except Text if OutputKeyClass
			if (null != currentJob.getOutputKeyClass())
				keyOutClass = currentJob.getOutputKeyClass();

			// setting anything except Text if OutputValueClass
			if (null != currentJob.getOutputValueClass())
				valueOutClass = currentJob.getOutputValueClass();

		} catch (Exception e1) {
			System.out.println("Error finding class in JVM "
					+ classVariableName);
			e1.printStackTrace();
			throw e1;
		}

	}

	/**
	 * Given a key and the hashed value of that key (by which name the folder exists)
	 * read the file using FileReaderIterator and invoke the reduce method
	 * */
	public void reducerHandlerForKey(Text key, String hashedKeyFolderName)
			throws Exception {
		System.out.println("Invoking reduce function");
		Method reduce;
		try {
			reduce = classVariable.getMethod("reduce", keyInClass,
					Iterable.class, contextClass);

			System.out.println("Reduce call for key " + key + " foldername "
					+ hashedKeyFolderName);
			Iterable<Object> iter = (Iterable<Object>) FileSys
					.readMapperOutputForKey(new Text(hashedKeyFolderName + ""),
							currentJob.getJobName(), localServerNumber);

			// give context variable
			reduce.invoke(objectInstance, key, iter, contextVariable);

		} catch (Exception e) {
			System.out.println("Exception in running map function");
			e.printStackTrace();
			throw e;
		}
	}

	/**
	 * Invokes the cleanup method. If not overridden, the invoked method does noop
	 * */
	private void reducerHandlerCleanup() throws Exception {
		try {
			System.out.println("\tCalling cleanup::Reducer "
					+ currentJob.getReducerClass().toString());
			phase = "CLEANUP";
			Method cleanup = classVariable.getMethod("cleanup", contextClass);

			System.out.println("Invoking cleanup function");

			// give context variable
			cleanup.invoke(objectInstance, contextVariable);

		} catch (Exception e) {

			System.out.println("exception in init of cleaup");
			System.out.println("No cleanup found for Reducer");
		}

	}

}
