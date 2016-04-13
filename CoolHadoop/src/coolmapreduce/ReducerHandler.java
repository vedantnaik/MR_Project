package coolmapreduce;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import fs.FileSys;
import utils.Constants;
import io.IntWritable;
import io.LongWritable;
import io.Text;

public class ReducerHandler {

	
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

	// Current Job variable
	Job currentJob;

	// String value indicating which phase the Job is in
	String phase = "";

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
	
	// TODO: set this value
	int localServerNumber;

	// get context from job for now class variable
	Context contextVariable = null;
	
	// Assumes each MapperHandler has a list of files to work
	// on
	
	public ReducerHandler() {
		currentJob = null;
	}
	
	public ReducerHandler(Job _currentJob){
		currentJob = _currentJob;
		contextVariable = new Context(currentJob, Constants.CTX_RED_PHASE, localServerNumber);
	}
	
	
	public void runReducerHandler() throws Exception {

		System.out.println("\tStarting Reducer for "
				+ currentJob.getReducerClass().toString());

		reducerHandlerInit();

		// calling map for each line

		// update accordingly
		phase = Constants.SETUP;
		reducerHandlerSetup();

		phase = Constants.RUNNING;
		//can we have number of keys?
		int total = 3;
		
		
		// iterate over the keys and call reduce for each key
		for (int i = 0; i < total ; i++) {
			// right now dummy key1
			reducerHandlerRun(new Text("the"));
			phase = Constants.RUNNING + " " + (i * 100.0) / total;
		}

		phase = Constants.CLEANUP;
		reducerHandlerCleanup();

		phase = Constants.MAP_FINISH;

	}
	
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
			e.printStackTrace();
			throw e;
		}

	}

	private void reducerHandlerInit() throws Exception {
		try {
			classVariableName = currentJob.getReducerClass().getName();
			classVariable = Class
					.forName(currentJob.getReducerClass().getName());
			objectInstance = classVariable.newInstance();

			
			// setting anything except Text if MapOutputKeyClass()
			if(null != currentJob.getMapOutputKeyClass())
				keyInClass = currentJob.getMapOutputKeyClass();		
				
			// setting anything except Text if MapOutputValueClass
			if(null != currentJob.getMapOutputValueClass())
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
	
	public void reducerHandlerRun(Text key) throws Exception {
		System.out.println("Invoking map function");
		Method reduce;
		try {
			reduce = classVariable.getMethod("reduce", keyInClass, Iterable.class,
					contextClass);


				 System.out.println("Reduce call for key " + key);
				Iterable<IntWritable> iter = (Iterable<IntWritable>) FileSys.readMapperOutputForKey(key, currentJob.getJobName(), 0);
				
				// give context variable
				reduce.invoke(objectInstance, key, iter,
						contextVariable);

			

		} catch (Exception e) {
			System.out.println("Exception in running map function");
			e.printStackTrace();
			throw e;

		}

	}

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
			e.printStackTrace();
			throw e;
		}
	
		
	}
	
	public void moveAndMergeTempOutput(){}
	
}
