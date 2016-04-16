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
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

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

import coolmapreduce.Job;

public class FileSys {
	
	
	/********************************************************************************************
	 * 
	 * FileSys is intended to be used as a util class without instantiating
	 * 
	 * ******************************************************************************************/

	
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
	 * */
	public static void moveMapperTempFilesToRemoteReducers(int localServerNumber, String mapKey, int foreignServerNumber, Job currentJob) throws SocketException, IOException, JSchException, SftpException{
		
		
		// TODO: TEST CREATING FOLDERS ON REMOTE SERVERS
		
		// check if destination folder exists
		String destFolderStr = Constants.ABSOLUTE_REDUCER_INPUT_FOLDER
										.replace("<JOBNAME>", currentJob.getJobName())
										.replace("<KEY>", mapKey);
		
		makeForeignFolderIfNotExist(destFolderStr, foreignServerNumber, currentJob);
		
		
		// copy file
		String srcFilePath = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE
										.replace("<JOBNAME>", currentJob.getJobName())
										.replace("<KEY>", mapKey)
										.replace("<SERVERNUMBER>", localServerNumber + "");
		
		String destFilePath = Constants.ABSOLUTE_REDUCER_INPUT_FILE
										.replace("<JOBNAME>", currentJob.getJobName())
										.replace("<KEY>", mapKey)
										.replace("<SERVERNUMBER>", localServerNumber + "");
		
		String destDns = currentJob.getConf().getServerIPaddrMap().get(new Integer(foreignServerNumber));
		
		scpCopy(srcFilePath, destFilePath, destDns);
	}
	
	
	/**
	 * GIVEN	source filename
	 * 			destination file path
	 * 			destination EC2 instance's IP
	 * Copies source file to destination on other server
	 * */
	// Reference : http://unix.stackexchange.com/questions/136165/java-code-to-copy-files-from-one-linux-machine-to-another-linux-machine
	public static void scpCopy(String fsrc, String fdest, String destIP) throws SocketException, IOException, JSchException, SftpException{
		JSch jsch = new JSch();
		jsch.addIdentity(Constants.PEM_FILE_PATH);
		
		Session session = null;
		session = jsch.getSession(Constants.EC2_USERNAME, destIP, Constants.SSH_PORT);
		
		session.setPassword("");
		session.setConfig("StrictHostKeyChecking", "no");
	    session.connect();
		
	    ChannelSftp channel = null;
		channel = (ChannelSftp)session.openChannel("sftp");
		channel.connect();
		
//		File localFile = new File(fsrc);
//		System.out.println("local file to copy name : " + fsrc + " localFileName " + localFile.getName());
//		channel.put("sampleSortPartTemp/"+localFile.getName() , "Project/sampleSortMyParts/"+localFile.getName());
		
		channel.put(fsrc, fdest);
	    
		channel.disconnect();
		session.disconnect();
	}
	
	
	
	
	
	
	
	
	/********************************************************************************************
	 * 
	 * 	LOCAL FILE OPERATIONS:
	 * 
	 * ******************************************************************************************/
	
	/**
	 * Write local file
	 * USAGE: recording output form Mapper : context.write 
	 *  
	 * ./output/JOBNAME/mapper/KEY/valuesSERVERNUMBER.txt
	 * */
	public static void writeMapperValueToKeyFolder(Integer keyHashCode, Object value, String jobName, int localServerNumber){
		
		String fileNameToWriteIn = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE
									.replace("<JOBNAME>", jobName)
									.replace("<KEY>", keyHashCode.toString())
									.replace("<SERVERNUMBER>", localServerNumber + "");
		
		String valuesFileName = "/values"+localServerNumber+".txt";
		File folderToWriteIn = new File(fileNameToWriteIn.replace(valuesFileName, ""));
		
//		System.out.println("\tfileNameToWriteIn " + fileNameToWriteIn + " with " + folderToWriteIn);
		
		if(!folderToWriteIn.isDirectory()){
			folderToWriteIn.mkdir();
//			System.out.println("\tMaking new Dir for Key " + keyHashCode);
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
			// Folder exists. Key previously seen on this EC2
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
	 * 
	 * */
	public static Iterator<Object> readMapperOutputForKey(Text key, String jobName, int localServerNumber) throws IOException{
		
		String fileToRead = Constants.RELATIVE_COMBINED_REDUCER_INPUT_FILE
									.replace("<JOBNAME>", jobName)
									.replace("<KEY>", key.toString());				

		FileReaderIterator iter = new FileReaderIterator(new File(fileToRead));

		return iter;

	}	
	
	
	/**
	 * reducerInputFilesCombiner
	 * 
	 * combine all 
	 *  RELATIVE_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values<SERVERNUMBER>.txt"
	 * to 
	 *  RELATIVE_COMBINED_REDUCER_INPUT_FILE = "./input/<JOBNAME>/reducer/<KEY>/values.txt";
	 *  
	 * deletes reducer input files
	 * @throws IOException 
	 * 
	 * 
	 * */
	
	public static void combineReducerInputFiles(Text key, String jobName, int localServerNumber) throws IOException{
		
		File reducerInputFolder = new File(Constants.RELATIVE_REDUCER_INPUT_FOLDER
												.replace("<JOBNAME>", jobName)
												.replace("<KEY>", key.toString()));
		
		
		
		File finalValuesFile = new File(Constants.RELATIVE_COMBINED_REDUCER_INPUT_FILE
												.replace("<JOBNAME>", jobName)
												.replace("<KEY>", key.toString()));
		
		ObjectOutputStream finalOOS = null;
		for (File valuesFile : reducerInputFolder.listFiles()){
			FileReaderIterator valuesIter = new FileReaderIterator(valuesFile); 
			
			for(Object valueObject : valuesIter){
				finalOOS = getOOS(finalValuesFile);
				finalOOS.writeObject(valueObject);
			}
			
			valuesFile.delete();
		}
		finalOOS.close();
	}
	
	
	/**
	 * Locally move mapper output values file to reducer input folder
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
								.replace("<KEY>", mapKey);
		
		FileSys.makeLocalFolderIfNotExist(destFolderStr);
		
		// move between dirs
		String srcFileStr = Constants.RELATIVE_MAPPER_CONTEXT_OUTPUT_FILE
										.replace("<JOBNAME>", jobName)
										.replace("<KEY>", mapKey)
										.replace("<SERVERNUMBER>", localServerNumber+"");
		
		String destFileStr = Constants.RELATIVE_REDUCER_INPUT_FILE
										.replace("<JOBNAME>", jobName)
										.replace("<KEY>", mapKey)
										.replace("<SERVERNUMBER>", localServerNumber+"");
		
		java.nio.file.Path src = java.nio.file.Paths.get(srcFileStr);
		java.nio.file.Path dest = java.nio.file.Paths.get(destFileStr);
		
		
		try {
			java.nio.file.Files.move(src, dest, REPLACE_EXISTING);
		} catch (IOException e) {
			System.err.println("UNABLE TO SHUFFLE FILE LOCALLY FOR KEY " + mapKey + " in ServerNumber " + localServerNumber);
			e.printStackTrace();
		}
		
	}
	

	
	/**
	 * On every EC2 instance, when in reducer phase, the context.write() writes a pair of
	 * Key Value pairs to  RELATIVE_REDUCER_OUTPUT_FILE = "./output/<JOBNAME>/reducer/part-XXXXX" file
	 * 
	 * USAGE: Context.write() when in CTX_RED_PHASE
	 * */
	public static void writeReducerOutputKeyValue(Text key, Object value, String currentJobName){
		
		String fileToWriteInStr = Constants.RELATIVE_REDUCER_OUTPUT_FILE
										.replace("<JOBNAME>", currentJobName);
		
		String tabSeparatedLineToWrite = key.toString() + "\t" + value.toString();
		
		// write the value
		try {
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

		PutObjectResult result = s3client
				.putObject(new PutObjectRequest(outputS3BucketName, 
												fileNameOnS3Bucket, 
												localFileToMove).withAccessControlList(acl));
		System.out.println("Result of move from Server-" + localServerNumber 
				+ " to output S3:\n"
				+ result);
		
		// delete local copy
		localFileToMove.delete();
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
}

