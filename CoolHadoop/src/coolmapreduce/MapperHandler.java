package coolmapreduce;

import io.Text;

import java.io.File;

import java.lang.reflect.Method;
import java.util.List;

import utils.Constants;
import fs.FileSys;

public class MapperHandler implements Runnable {

	/**
	 * TODO: MapperHandler Requirement
	 * 
	 * 1. We need Job's information 2. For that Job, get the Mapper InputPath
	 * Context
	 * 
	 * 3. For each file - Read [see format, based on that reader to get each
	 * line] - For each line, call map on Mapper implementation Send context
	 * along
	 * 
	 * 4. Notify main class after all lines from all files are read.
	 * 
	 * */
	
	
	// List of Mapper Files
	List<File> listOfMapperFiles;
	
	// Current Job variable
	Job currentJob;
	
	// String value indicating which phase the Job is in
	String phase = "";

	// Class variable for Keeping track of Mapper class
	Class<?> classVariable = null;
	String classVariableName = "";
	
	// Actual instance of the Mapper
	Object objectInstance = null;
	
	// Object value sent along with each line for map phase
	Object lineObject;

	// default values for each KI, VI, KO, VO
	// KI = keyIn, VI = ValueIn
	// KO = KeyOut, VO = ValueOut
	Class<?> keyInClass = Object.class;
	Class<?> valueInClass = Text.class;
	Class<?> keyOutClass = Text.class;
	Class<?> valueOutClass = Text.class;
	Class<?> contextClass = Context.class;
	

	// get context from job for now class variable
	Context contextVariable = new Context();

	// Assumes each MapperHandler has a list of files to work
	// on
	public MapperHandler(List<File> _files, Job _job) {
		listOfMapperFiles = _files;
		currentJob = _job;
	}

	@Override
	public void run() {

		System.out.println("\tStarting Mapper for "
				+ currentJob.getMapperClass().toString());

		mapperHandlerInit();

		// calling map for each line

		// update accordingly
		phase = Constants.SETUP;
		mapperHandlerSetup();

		phase = Constants.RUNNING;
		int total = listOfMapperFiles.size();
		for (int i = 0; i < listOfMapperFiles.size(); i++) {
			mapperHandlerRun(listOfMapperFiles.get(i));
			phase = Constants.RUNNING + " " + (i * 100.0) / total;
		}

		phase = Constants.CLEANUP;
		mapperHandlerCleanup();

		phase = Constants.MAP_FINISH;

	}

	public void mapperHandlerRun(File file) {
		System.out.println("Invoking map function");
		Method map;
		try {
			map = classVariable.getMethod("map", keyInClass, keyOutClass,
					contextClass);

			for (String line : FileSys
					.readInputStringsFromLocalInputBucket(file.getPath())) {

				// System.out.println("Reading " + line);

				// give context variable
				map.invoke(objectInstance, lineObject, new Text(line),
						contextVariable);

			}

		} catch (Exception e) {
			System.out.println("Exception in running map function");
			e.printStackTrace();
			System.exit(0);

		}

	}

	public void mapperHandlerInit() {
		try {
			classVariableName = currentJob.getMapperClass().getName();
			classVariable = Class
					.forName(currentJob.getMapperClass().getName());
			objectInstance = classVariable.newInstance();

			// always Object class
			keyInClass = Object.class;

			// Always Text class perhaps
			valueInClass = Text.class;

			// setting anything except Text if MapOutputKeyClass
			if (null != currentJob.getMapOutputKeyClass())
				keyOutClass = currentJob.getMapOutputKeyClass();

			// setting anything except Text if MapOutputValueClass
			if (null != currentJob.getMapOutputValueClass())
				valueOutClass = currentJob.getMapOutputValueClass();

		} catch (Exception e1) {
			System.out.println("Error finding class in JVM "
					+ classVariableName);
			e1.printStackTrace();
			System.exit(0);
		}
	}

	public void mapperHandlerSetup() {
		// setup phase
		try {

			System.out.println("\tCalling setup::Mapper for " + classVariable);
			phase = "SETUP";
			Method setup = classVariable.getMethod("setup", contextClass);

			System.out.println("Invoking setup function");

			// give context variable
			setup.invoke(objectInstance, contextVariable);

		} catch (Exception e) {
			System.out.println("exception in init of setup");
			e.printStackTrace();
			System.exit(0);
		}

	}

	public void mapperHandlerCleanup() {
		try {
			System.out.println("\tCalling cleanup::Mapper "
					+ currentJob.getMapperClass().toString());
			phase = "CLEANUP";
			Method cleanup = classVariable.getMethod("cleanup", contextClass);

			System.out.println("Invoking cleanup function");

			// give context variable
			cleanup.invoke(objectInstance, contextVariable);

		} catch (Exception e) {

			System.out.println("exception in init of cleaup");
			e.printStackTrace();
			System.exit(0);
		}
	}

	public String returnPhaseStatus() {
		return phase;
	}
}
