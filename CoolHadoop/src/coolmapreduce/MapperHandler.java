package coolmapreduce;

import fs.FileSys;
import io.Text;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import utils.Constants;

public class MapperHandler {

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
	List<String> listOfMapperFiles;
	
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

	// TODO: set this value
	int localServerNumber;

	// get context from job for now class variable
	Context contextVariable = null;

	// Assumes each MapperHandler has a list of files to work
	// on
	public MapperHandler(List<String> _files, Job _job) {
		listOfMapperFiles = _files;
		currentJob = _job;
	}
	
	/**
	 * Init MapperHandler with an empty List of Mapper files
	 */
	public MapperHandler() {
		listOfMapperFiles = new ArrayList<>();
		currentJob = null;
	}
	
	/**
	 * Init MapperHandler with empty list of Mapper files
	 * and currentJob as the Job object
	 * @param _currentJob
	 * the Job object to init the MapperHandler with
	 * localServerNumber : server on which this mapper instance is created
	 */
	public MapperHandler(Job _currentJob, int _localServerNumber){
		listOfMapperFiles = new ArrayList<>();
		currentJob = _currentJob;
		localServerNumber = _localServerNumber;
		contextVariable = new Context(currentJob, Constants.CTX_MAP_PHASE, localServerNumber);
	}

	/**
	 * Serially run the MapperHandler calling the functions
	 * <p><ul>
	 * <li> 1. Call mapper Handler Init - INIT of Mapper
	 * <li> 2. Call the Setup of Mapper - the User's Mapper setup function
	 * <li> 3. Call map function of the user, line-by-line on each file 
	 * <li> 4. Call the mapper's cleanup function defined by User
	 * </ul></p>
	 * @throws Exception
	 */
	public void runMapperHandler() throws Exception {

		System.out.println("\tStarting Mapper for "
				+ currentJob.getMapperClass().toString());

		mapperHandlerInit();

		// calling map for each line

		// update accordingly
		phase = Constants.SETUP;
		mapperHandlerSetup();

		phase = Constants.RUNNING;
		int total = listOfMapperFiles.size();
		System.out.println("mapper files " + total);
		for (int i = 0; i < listOfMapperFiles.size(); i++) {
			mapperHandlerRun(listOfMapperFiles.get(i));
			phase = Constants.RUNNING + " " + (i * 100.0) / total;
		}

		phase = Constants.CLEANUP;
		
		mapperHandlerCleanup();

		// MKM processing after cleanup
		writeMapperKeysMapToFile();

		moveMapperKeyMapFileToMainServer();
		
		phase = Constants.MAP_FINISH;

	}

	public void mapperHandlerRun(String file) throws Exception {
		System.out.println("Invoking map function");
		Method map;
		try {
			System.out.println("invoking map ");
			map = classVariable.getMethod("map", keyInClass, keyOutClass,
					contextClass);

				for (String line : FileSys
						.readGZippedInputStringsFromInputS3Bucket(
								currentJob.getConf().get(Constants.INPUT_BUCKET_NAME),
								file)) {

				// System.out.println("Reading " + line);

				// give context variable
				map.invoke(objectInstance, lineObject, new Text(line),
						contextVariable);

			}

		} catch (Exception e) {
			System.out.println("Exception in running map function");
			e.printStackTrace();
			throw e;

		}

	}

	public void mapperHandlerInit() throws Exception {
		System.out.println("Mapper");
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
			throw e1;
		}
	}

	public void mapperHandlerSetup() throws Exception {
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
			throw e;
		}

	}

	public void mapperHandlerCleanup() throws Exception {
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
			throw e;
		}
	}

	// File system interactions
	
	/**
	 * Writes the MKM (explained in Context.java) to 
	 * RELATIVE_MAPPER_CONTEXT_OUTPUT_FOLDER ./output/JOBNAME/mapper/
	 * 
	 * e.g. file created for wordCount on server 1:
	 * 		./output/wordCount/mapper/mkmonserver1
	 * */
	public void writeMapperKeysMapToFile(){
		String fileToWriteStr = Constants.RELATIVE_MAPPER_KEY_MAPS_FOLDER
											.replace("<JOBNAME>", currentJob.getJobName())
											+ Constants.MKM_FILE_NAME
													.replace("<SERVERNUMBER>", ""+localServerNumber);

		System.out.println("Write mapper key map for " + currentJob.getJobName() + " on server " + localServerNumber);
		
		FileSys.writeObjectToFile(contextVariable.getMapperKeysMap(), fileToWriteStr);
	} 
	
	/**
	 * 1. Move mapper key map file to the main server at 
	 * 		ABSOLUTE_MAPPER_KEY_MAPS_FOLDER = "~/Project/output/JOBNAME/MKMs/"
	 * 
	 * 		e.g. server 1 will write this file to main server's local disk 
	 * 		"~/Project/output/testJob/MKMs/mkmonserver1"
	 * 
	 * 2. delete local copy from this ec2 instance
	 * 
	 * */
	public void moveMapperKeyMapFileToMainServer(){
		
		String fsrcStr =  Constants.RELATIVE_MAPPER_KEY_MAPS_FOLDER
									.replace("<JOBNAME>", currentJob.getJobName())
									+ Constants.MKM_FILE_NAME
										.replace("<SERVERNUMBER>", ""+localServerNumber);
		
		String fdestStr = Constants.ABSOLUTE_MASTER_MAPPER_KEY_MAPS_FOLDER
									.replace("<JOBNAME>", currentJob.getJobName())
									+ Constants.MKM_FILE_NAME
										.replace("<SERVERNUMBER>", ""+localServerNumber);
		
		String destIP = currentJob.getConf().getServerIPaddrMap().get(Constants.MASTER_SERVER_IP_KEY);
		
		try {
			FileSys.scpCopy(fsrcStr, fdestStr, destIP);
			
			// TODO Delete local copy
		} catch (SocketException e) {
			System.err.println("Socket error while trying to send MKM object file to master server from server " + localServerNumber);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Unable to send MKM file to Master server from server " + localServerNumber);
			e.printStackTrace();
		} catch (JSchException e) {
			System.err.println("JSch Exception while trying to move MKM file from server " + localServerNumber);
			e.printStackTrace();
		} catch (SftpException e) {
			System.err.println("Sftp Exception while trying to move MKM file from server " + localServerNumber);
			e.printStackTrace();
		}
	}
	
	
	public String returnPhaseStatus() {
		return phase;
	}

	public List<String> getListOfMapperFiles() {
		return listOfMapperFiles;
	}

	public void setListOfMapperFiles(List<String> listOfMapperFiles) {
		this.listOfMapperFiles = listOfMapperFiles;
	}

	public Job getCurrentJob() {
		return currentJob;
	}

	public void setCurrentJob(Job currentJob) {
		this.currentJob = currentJob;
	}
	
	public boolean addToListOfMapperFiles(String _file){
		return listOfMapperFiles.add(_file);
	}
	
}
