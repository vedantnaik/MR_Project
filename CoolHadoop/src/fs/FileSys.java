package fs;

import fs.iter.FileReaderIterator;
import io.Text;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;

import utils.Constants;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import coolmapreduce.Context;
import coolmapreduce.Job;
import coolmapreduce.MapperHandler;
/**
 * 
 * @author Vedant_Naik, Vaibhav_Tyagi, Dixit_Patel, Rohan_Joshi
 *
 */
public class FileSys {
	
	/********************************************************************************************
	 * 
	 * FileSys is intended to be used as a util class without instantiating
	 * 
	 * ******************************************************************************************/
	
	/********************************************************************************************
	 * 
	 * 	S3 FILE OPERATIONS
	 * 
	 * ******************************************************************************************/

	
	/**
	 * Read from S3 bucket
	 * 
	 * File type is .gz
	 * 
	 * @param 
	 * inputBucketName : S3 bucket where input files are stored
	 * fileObjectkey : An input file object key
	 * 
	 * @return
	 * An ArrayList of Strings where each line from input file is stored in the list		  	
	 * */
	public static ArrayList<String> readGZippedInputStringsFromInputS3Bucket(String inputBucketName, String fileObjectKey){
		ArrayList<String> dataRecordList = new ArrayList<String>();
		try {
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			S3Object s3object = s3client.getObject(new GetObjectRequest(inputBucketName, fileObjectKey));
			
			GZIPInputStream gzipStream = new GZIPInputStream(s3object.getObjectContent());
			BufferedReader buffered = new BufferedReader(new InputStreamReader(gzipStream, "ASCII"));
			
			String fileLine;
			while((fileLine = buffered.readLine())!=null){
				dataRecordList.add(fileLine);
			}
			
			buffered.close();
			gzipStream.close();
		} catch (IOException e) {
			System.out.println("Unable to read file from Input S3 Bucket");
			e.printStackTrace();
		}
		
		return dataRecordList;
	}
	
	/**
	 * 
	 * Read from local file systems
	 * 
	 * File type is .gz
	 * 
	 * @param 
	 * inputBucketName : S3 bucket where input files are stored
	 * fileObjectkey : An input file object key
	 * 
	 * @return
	 * An ArrayList of Strings where each line from input file is stored in the list
	 * */
	public static ArrayList<String> readInputStringsFromLocalInputBucket(String folderPath, String fileName){
		ArrayList<String> dataRecordList = new ArrayList<String>();
		String fileToReadName = folderPath + "/" + fileName;
		try {
			GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(new File(fileToReadName)));
			BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "ASCII"));
			String fileLine;
			
			while(null != (fileLine = reader.readLine())){
				dataRecordList.add(fileLine);
			}
			
			reader.close();
			gzipStream.close();
		} catch (FileNotFoundException e) {
			System.err.println("Unable to read local file " + fileToReadName);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Unable to read line from local copy of file " + fileToReadName);
			e.printStackTrace();
		}
		return dataRecordList;
	}
	
	
	/**
	 * Given context and a folder location on output S3 bucket, move the given localFile to that location
	 * */
	public static void moveToFolderOnOutputBucket(Context context, String folderPath, File localFile){
		String fileNameOnS3Bucket = folderPath + "/" + localFile.getName();
		
		// Move to S3 bucket
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		AccessControlList acl = new AccessControlList();
		acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
		
		System.out.println("Moving file to S3 bucket: " + localFile.getName() + " to S3 location " + fileNameOnS3Bucket);
		
		PutObjectResult result = s3client
		.putObject(new PutObjectRequest(context.getConfiguration().get(Constants.OUTPUT_BUCKET_NAME), 
										fileNameOnS3Bucket, 
										localFile).withAccessControlList(acl));
		System.out.println("Result of move " + localFile 
		+ " to output S3:\n"
		+ result);
	}
	
	
	/**
	 * Given context and a folder location on output S3 bucket and a file in that folder,
	 * copy the file to local disk
	 * */
	public static InputStream getInputStreamForFileFromBucket(Context context, String folderPath, File fileToCopy){
				
		ArrayList<String> dataRecordList = new ArrayList<String>();
		try {
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			System.out.println("getting outputbucket" + context.getConfiguration().get(Constants.OUTPUT_BUCKET_NAME) );
			System.out.println("reading " + folderPath + " filetoCopy " + fileToCopy);
			S3Object s3object = s3client
					.getObject(new GetObjectRequest(context.getConfiguration().get(Constants.OUTPUT_BUCKET_NAME), 
							folderPath + "/" + fileToCopy));
			
			return new ObjectInputStream(s3object.getObjectContent());
			
		} catch (IOException e) {
			System.out.println("Unable to read file from Input S3 Bucket");
			e.printStackTrace();
		}
		return null;
	}
	
	/********************************************************************************************
	 * 
	 * FILE OPERATIONS BETWEEN TWO EC2 INSTANCES
	 * 
	 * ******************************************************************************************/

	/**
	 * @param
	 * mapKey				:		key for which files are to be moved from this EC2 instance
	 * foreignServerNumber	:		server number to where the source file is to be moved
	 * 
	 * This method moves the file from one EC2 instance to another
	 * @throws SftpException 
	 * @throws JSchException 
	 * @throws IOException 
	 * @throws SocketException 
	 * @throws InterruptedException 
	 * */
	public static void moveMapperTempFilesToRemoteReducers(int localServerNumber, String mapKey, int foreignServerNumber, Job currentJob) throws SocketException, IOException, JSchException, SftpException{
		
		String destFolderStr = Constants.ABS_REDUCER_INPUT_FOLDER
										.replace("<JOBNAME>", currentJob.getJobName())
										.replace("<KEY>", mapKey.toString());
		
		String srcFilePath = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE
										.replace("<JOBNAME>", currentJob.getJobName())
										.replace("<KEY>", mapKey.toString())
										.replace("<SERVERNUMBER>", localServerNumber + "");
		
		String destFilePath = Constants.ABSOLUTE_REDUCER_INPUT_FILE
										.replace("<JOBNAME>", currentJob.getJobName())
										.replace("<KEY>", mapKey.toString())
										.replace("<SERVERNUMBER>", localServerNumber + "");
		
		String destDns = currentJob.getConf().getServerIPaddrMap().get(new Integer(foreignServerNumber));
		System.out.println("move mapper files to remote location from " + srcFilePath
			+ " to " + destFilePath + " destDns " + destDns);
		
		if (new File(srcFilePath).exists()) {
			scpCopy(srcFilePath, destFilePath, destDns);
		}else{
			System.out.println("This file doesn't exists");
		}
	}
	
	
	/**
	 * GIVEN	source filename
	 * 			destination file path
	 * 			destination EC2 instance's IP
	 * Copies source file to destination on other server
	 * 
	 * Waits and retries if channel does not open OR if unable to put file on channel
	 * 
	 * @throws JSchException 
	 * @throws InterruptedException 
	 * Reference : http://unix.stackexchange.com/questions/136165/java-code-to-copy-files-from-one-linux-machine-to-another-linux-machine
	 * */
	public static void scpCopy(String fsrc, String fdest, String destIP) throws SocketException, JSchException{
		JSch jsch = new JSch();
		jsch.addIdentity(Constants.PEM_FILE_PATH);
		
		Session session = null;
		session = jsch.getSession(Constants.EC2_USERNAME, destIP);
		
		session.setPassword("");
		session.setConfig("UserKnownHostsFile", "/dev/null");
		session.setConfig("StrictHostKeyChecking", "no");
	    session.connect();
		
	    ChannelSftp channel = null;
	    boolean channeldone = false;
	    while(!channeldone){
	    	
	    	try{
	    		channel = (ChannelSftp)session.openChannel("sftp");
	    		channel.connect();
	    	}catch(Exception e){
				System.out.println("Continue channel opening");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					System.out.println("Unable to put thread on sleep");
				}
				continue;				
			}
	    	channeldone = true;
			System.out.println("waiting for channel open " + channeldone);
	    }

		
		boolean done = false;
		while(!done){
			try{
				channel.put(fsrc, fdest);
			}catch(Exception e){
				System.out.println("Continue JSCP");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					System.out.println("Unable to put thread on sleep");
				}
				continue;				
			}
			done = true;
			System.out.println("waitfaing " + done);
		}
		
		channel.disconnect();
		session.disconnect();
		jsch = null;
	}
	
	
	/********************************************************************************************
	 * 
	 * 	LOCAL FILE OPERATIONS:
	 * 
	 * ******************************************************************************************/
	
	/**
	 * Write given object to a local file
	 * */
	public static void writeObjectToFile(Object objectToWrite, String relativeFilePathStr){
		try {
			ObjectOutputStream oos =  new ObjectOutputStream(new FileOutputStream(new File(relativeFilePathStr)));
			oos.writeObject(objectToWrite);
			oos.flush();
			oos.close();
		} catch (IOException e) {
			System.err.println("Unable to write object intended to be written in the " + relativeFilePathStr + " file");
			e.printStackTrace();
		}	
	}
	
	/**
	 * Write local file
	 * USAGE: recording output form Mapper : context.write 
	 *  
	 * ./output/JOBNAME/mapper/KEY/valuesSERVERNUMBER.txt
	 * */
	public static void writeMapperValueToKeyFolder(String keyHashCode, Object value, String jobName, int localServerNumber){
		
		String fileNameToWriteIn = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE
									.replace("<JOBNAME>", jobName)
									.replace("<KEY>", keyHashCode.toString())
									.replace("<SERVERNUMBER>", localServerNumber + "");
		
		String valuesFileName = "/values"+localServerNumber+".txt";
		File folderToWriteIn = new File(fileNameToWriteIn.replace(valuesFileName, ""));
		
		if(!folderToWriteIn.isDirectory()){
			folderToWriteIn.mkdir();
			try {
				ObjectOutputStream oos = getOOS(new File(fileNameToWriteIn));
		        oos.writeObject(value);
		        oos.flush();
		        oos.close();
			} catch (IOException e) {
				System.out.println("Could not do a context write from mapper");
				e.printStackTrace();
			}
		} else {
			try {
				ObjectOutputStream oos = getOOS(new File(fileNameToWriteIn));
		        oos.writeObject(value);
		        oos.flush();
		        oos.close();
		    } catch (IOException e) {
				System.out.println("Could not do a context write from mapper");
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * Read from local file Constants.RELATIVE_COMBINED_REDUCER_INPUT_FILE
	 * 
	 * Assuming the values<SERVERNUMBER>.txt files are all combined to values.txt
	 * 
	 * USAGE: Feeding input to reduce  
	 * 
	 * @return
	 * An iterator to traverse that file
	 * @throws IOException 
	 * */
	public static Iterable<Object> readMapperOutputForKey(Text key, String jobName, int localServerNumber) throws IOException{
		
		String fileToRead = Constants.RELATIVE_COMBINED_REDUCER_INPUT_FILE
									.replace("<JOBNAME>", jobName)
									.replace("<KEY>", key.toString());				

		FileReaderIterator iter = new FileReaderIterator(new File(fileToRead));

		return iter;
	}	
	
	
	/**
	 * combine all 
	 *  RELATIVE_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt"
	 * to 
	 *  RELATIVE_COMBINED_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values.txt";
	 *  
	 * deletes reducer input files
	 * @throws IOException 
	 * */
	
	public static void combineReducerInputFiles(Text key, String jobName, int localServerNumber) throws IOException{
		File reducerInputFolder = new File(Constants.RELATIVE_REDUCER_INPUT_FOLDER
												.replace("<JOBNAME>", jobName)
												.replace("<KEY>", key.toString()));
		
		System.out.println("reducerInput Folder " + reducerInputFolder);
		
		
		File finalValuesFile = new File(Constants.RELATIVE_COMBINED_REDUCER_INPUT_FILE
												.replace("<JOBNAME>", jobName)
												.replace("<KEY>", key.toString()));
		
		
		ObjectOutputStream finalOOS = null;
		for (File valuesFile : reducerInputFolder.listFiles()){
			FileReaderIterator<Object> valuesIter = new FileReaderIterator<Object>(valuesFile); 
			for(Object valueObject : valuesIter){
				finalOOS = getOOS(finalValuesFile);
				finalOOS.writeObject(valueObject);

				if(null != finalOOS){
					finalOOS.close();
				}
			}
			valuesIter.close();
			valuesFile.delete();
		}
	}
	
	
	/**
	 * Locally move mapper output values file to reducer input folder iff exists
 	 * mapKey				:		key for which files are to be moved from this EC2 instance
	 * localServerNumber	:		server number within which the values file needs to be moved
	 * currentJob			: 		current Job object
	 * 
	 * */
	public static void moveMapperTempFilesToLocalReducer(String mapKey, int localServerNumber, Job currentJob){
	
		String jobName = currentJob.getJobName();
		
		// make dir if does not exist
		String destFolderStr = Constants.RELATIVE_REDUCER_INPUT_FOLDER
								.replace("<JOBNAME>", jobName)
								.replace("<KEY>", mapKey.toString());
		

		FileSys.makeLocalFolderIfNotExist(destFolderStr);
		
		// move between dirs
		String srcFileStr = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE
										.replace("<JOBNAME>", jobName)
										.replace("<KEY>", mapKey.toString())
										.replace("<SERVERNUMBER>", localServerNumber+"");
		
		String destFileStr = Constants.RELATIVE_REDUCER_INPUT_FILE
										.replace("<JOBNAME>", jobName)
										.replace("<KEY>", mapKey.toString())
										.replace("<SERVERNUMBER>", localServerNumber+"");
		
		java.nio.file.Path src = java.nio.file.Paths.get(srcFileStr);
		
		if(new File(srcFileStr).exists()){
			java.nio.file.Path dest = java.nio.file.Paths.get(destFileStr);
			
			try {
				java.nio.file.Files.move(src, dest, REPLACE_EXISTING);
			} catch (IOException e) {
				System.err.println("UNABLE TO SHUFFLE FILE LOCALLY FOR KEY " + mapKey + " in ServerNumber " + localServerNumber);
				e.printStackTrace();
			}
		}		
	}
	
	/**
	 * On every EC2 instance, when in reducer phase, the context.write() writes a pair of
	 * Key Value pairs to  RELATIVE_REDUCER_OUTPUT_FILE = "./output/<JOBNAME>/reducer/part-XXXXX" file
	 * 
	 * USAGE: Context.write() when in CTX_RED_PHASE
	 * */
	public static void writeReducerOutputKeyValue(Text key, Object value, String currentJobName, int currentServerNumber){
		
		String fileToWriteInStr = Constants.RELATIVE_REDUCER_OUTPUT_FILE
										.replace("<JOBNAME>", currentJobName);
		
		String tabSeparatedLineToWrite = key.toString() + "\t" + value.toString() + "\n";
		
		// write the value
		try {
			
			// create folder
			//make output folder
			String outputFolderName = Constants.RELATIVE_REDUCER_OUTPUT_FOLDER.replace("<JOBNAME>", currentJobName);
			System.out.println("create output job Folder " + outputFolderName);
			File outputFolderFileObj = new File(outputFolderName);
			outputFolderFileObj.mkdir();

			System.out.println("create file on server " + fileToWriteInStr);
			// create empty file for part-0000X
			
			File partX = new File(fileToWriteInStr);
			partX.createNewFile();
			
			java.nio.file.Files.write(
					java.nio.file.Paths.get(fileToWriteInStr), 
					tabSeparatedLineToWrite.getBytes(), 
					StandardOpenOption.APPEND);
		} catch (IOException e) {
			System.err.println("UNABLE TO WRITE TO REDUCER OUTPUT FILE " + fileToWriteInStr);
			e.printStackTrace();
		}
	}
	
	
	/**
	 * This method moves the complete final output from this reducer, to the output S3 bucket
	 * 
	 * USAGE: Should be called by ReducerHandler at the end of all reduce() and cleanup() calls on that server
	 * 
	 * Actions:
	 * 1. Pick file for given job name
	 * 2. move this file to S3 output bucket
	 *  Rename the file to be moved 
	 * 	Such that the final digits identify the server from which this output was generated
	 * 	e.g. from "part-XXXXX" to "part-00001" for server 1
	 * 3. Delete part-XXXX file from the local disk
	 * 
	 * @param
	 * outputS3BucketName
	 * outputS3FolderName
	 * currentJobName
	 * localServerNumber
	 * */
	public static void moveFinalReducerOutputToS3Bucket(String outputS3BucketName, String outputS3FolderName, String currentJobName, int localServerNumber){

		String localFileToMoveStr = Constants.RELATIVE_REDUCER_OUTPUT_FILE
									.replace("<JOBNAME>", currentJobName);
		
		// Rename
		String serverNum = localServerNumber+"";
		String fileNameOnS3Bucket = outputS3FolderName + "/part-" + ("00000" + serverNum).substring(serverNum.length());
		
		// Move to S3 bucket
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		AccessControlList acl = new AccessControlList();
		acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
		
		File localFileToMove = new File(localFileToMoveStr);
		System.out.println("Moving reducer output part file from server : " + localServerNumber);

		if (new File(localFileToMoveStr).exists()) {
			
			PutObjectResult result = s3client.putObject(new PutObjectRequest(
					outputS3BucketName, fileNameOnS3Bucket, localFileToMove)
					.withAccessControlList(acl));
			
			System.out.println("Result of move from Server-"
					+ localServerNumber + " to output S3:\n" + result);
			// delete local copy
			localFileToMove.delete();
		}
	}
	
	///////////////////////////////
	// HELPERS
	///////////////////////////////
	
	/**
	 * Class used to append a serializable object to file
	 * 
	 * http://stackoverflow.com/questions/2094637/how-can-i-append-to-an-existing-java-io-objectstream
	 * */
	private static ObjectOutputStream getOOS(File storageFile) throws IOException {
		if (storageFile.exists()) {
		    // this is a workaround so that we can append objects to an existing file
		    return new AppendableObjectOutputStream(new FileOutputStream(storageFile, true));
		} else {
		    return new ObjectOutputStream(new FileOutputStream(storageFile));
		}
	}
	
	private static class AppendableObjectOutputStream extends ObjectOutputStream {

	    public AppendableObjectOutputStream(OutputStream out) throws IOException {
	        super(out);
	    }

	    @Override
	    protected void writeStreamHeader() throws IOException {
	        // do not write a header
	    }
	}

	/**
	 * Used to read appended serializable object from file
	 * 
	 * http://stackoverflow.com/questions/2094637/how-can-i-append-to-an-existing-java-io-objectstream
	 * */
	public static ObjectInputStream getOIS(FileInputStream fis)
            throws IOException {
		long pos = fis.getChannel().position();
		return pos == 0 ? new ObjectInputStream(fis) : 
		    new AppendableObjectInputStream(fis);
	}
	
	private static class AppendableObjectInputStream extends ObjectInputStream {

        public AppendableObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected void readStreamHeader() throws IOException {
            // do not read a header
        }
    }
	
	
	/**
	 * Given 
	 * - a destination folder path in string on another EC2 instance
	 * - server number of another EC2 instance
	 * - current MR job object
	 * 
	 * Effect
	 * - Should create the folder if it does not already exist
	 * 
	 * USAGE:
	 * to create necessary folder to which we scp files
	 * 
	 * */
	public static void makeForeignFolderIfNotExist(String destFolderStr, int foreignServerNumber, Job currentJob) throws JSchException, SftpException{
		JSch jsch = new JSch();
		jsch.addIdentity(Constants.PEM_FILE_PATH);
		
		Session session = null;
		session = jsch.getSession(
				Constants.EC2_USERNAME, 
				currentJob.getConf().getServerIPaddrMap().get(new Integer(foreignServerNumber)), 
				Constants.SSH_PORT);
		
		session.setPassword("");
		session.setConfig("StrictHostKeyChecking", "no");
	    session.connect();
		
	    ChannelSftp channel = null;
		channel = (ChannelSftp)session.openChannel("sftp");
		channel.connect();
		System.out.println("making foreign folder " + destFolderStr + " on " + foreignServerNumber);
		if(null == channel.stat(destFolderStr)){
			channel.mkdir(destFolderStr);
		}
		
		channel.disconnect();
		session.disconnect();
	}
	
	public static void makeLocalFolderIfNotExist(String relativeDirPath){
		File destFolder = new File(relativeDirPath);
		if(!destFolder.exists()){
			destFolder.mkdirs();
		}
	}


	/**
	 * Effect : merge all valuesX.txt files from all servers from map phase to create final values.txt
	 *  for all keys
	 * 
	 * Usage : during start of reduce phase to merge all valuesX.txt for all keys
	 * */
	public static void mergeValuesForAllKeysForJob(Job currentJob, int serverNumber, HashMap<String, Object> mapReadFromMKMs,
			Map<String, Object>  masterKeyServerMap) {
		try {
			// for each key combine values
			for (String keyFolder : mapReadFromMKMs.keySet()){
				
				if((int) masterKeyServerMap.get(keyFolder) == serverNumber){
					System.out.println("combiner for " + keyFolder);
					FileSys.combineReducerInputFiles(new Text(keyFolder+""), currentJob.getJobName(), serverNumber);
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println("Could not find the master MKM file while combining input valuesX.txt");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error while reading/writing to files while combining input valuesX.txt");
			e.printStackTrace();
		}
	}
	
	public static void makeFolderStructureOnMaster(String jobName) throws IOException{
			
		//delete jobname folder under output
		String outputJobFolderName = Constants.ABSOLUTE_JOB_FOLDER.replace("<JOBNAME>", jobName);
		System.out.println("recursively deleting " + outputJobFolderName);
		FileUtils.deleteDirectory(new File(outputJobFolderName));
		
		// make again
		System.out.println("create output job Folder " + outputJobFolderName);
		File outputJobFolder = new File(outputJobFolderName);
		outputJobFolder.mkdir();
		
		// make MasterMKMs folder
		String masterMKMfolder = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER.replace("<JOBNAME>", jobName);
		System.out.println("create masterMKMs Folder " + masterMKMfolder);
		File masterMKMfolderObj = new File(masterMKMfolder);
		masterMKMfolderObj.mkdir();
	}
}
