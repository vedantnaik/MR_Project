package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import datafile.DataFileParser;
import datafile.DataRecord;

public class FileSystem {
	
	private class Constants {
		private static final String PEM_FILE_PATH = "credentials/MyKeyPair.pem";
		public static final String PUBLIC_DNS_FILE = "publicDnsFile.txt";
		public static final String EC2_USERNAME = "ubuntu";
		public static final int SSH_PORT = 22;

		public static final String SAMPLESORT_PART_TEMP_FOLDER = "sampleSortPartTemp";
		public static final String SAMPLESORT_MY_PART = "~/Project/sampleSortMyParts";
		public static final String SAMPLESORT_MY_PART_RELATIVE_FOLDER = "sampleSortMyParts";
		
		public static final String S3_OUTPUT_PART_TEMP_FILE = "s3OutputPartTemp";
		public static final String MyS3BucketOuputPart_DistributedEC2Sort_FOLDER = "DistributedEC2Sort";
		
	}
	
	String inputBucketName;
	String outputBucketName;
	String fileObjectKey;
	ArrayList<String> fileNameSizeList_GLOBAL;
	static AWSCredentials credentials;
	static AmazonS3 s3client;
	static AccessControlList acl;
	// EC2 Specific data
	public static HashMap<Integer, String> serverIPaddrMap;
	
	public FileSystem(String inputBucketName, String outputBucketName) throws IOException{
		this.inputBucketName = inputBucketName;
		this.outputBucketName = outputBucketName;
		this.fileNameSizeList_GLOBAL = new ArrayList<String>();
		
		credentials = new ProfileCredentialsProvider().getCredentials();
		s3client = new AmazonS3Client(credentials);
		
		// adding permissions
		acl = new AccessControlList();
		acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
		
		ObjectListing objList = s3client.listObjects(inputBucketName);
	
		// EC2 specific
		serverIPaddrMap = new HashMap<Integer, String>();
		addIPaddrsFromPublicDnsFile();
		
		///////////////////////////////////////////////
		// TODO: REMOVE AFTER DEBUG!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
//		int debug_limitCount = 0;
		
		for(S3ObjectSummary objSum : objList.getObjectSummaries() ){
			if(objSum.getKey().contains("climate") && objSum.getKey().contains("txt.gz")){
				this.fileNameSizeList_GLOBAL.add(objSum.getKey() + ":" + objSum.getSize());
//				debug_limitCount++;
			}
//			if(debug_limitCount == 10) {break;}
		}
		
	}
	
	public FileSystem() {
	}

	/**
	 * Constructor helper to read IPs of all servers into Map
	 * @throws IOException 
	 * */
	private void addIPaddrsFromPublicDnsFile() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(Constants.PUBLIC_DNS_FILE));
		String line;
		int serverNumber = 0;
		while((line = br.readLine()) != null){
			serverIPaddrMap.put(serverNumber++, line.trim());
		}
		br.close();
	}



	/****************************************************************
	 * 
	 * 						AMAZON (S3) FILE SYSTEM
	 * 
	 ****************************************************************/
	
	public static ArrayList<DataRecord> readInputDataRecordsFromInputBucket(String inputBucketName, String fileObjectKey){
	
		ArrayList<DataRecord> dataRecordList = new ArrayList<DataRecord>();
		
		try {
			
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			S3Object s3object = s3client.getObject(new GetObjectRequest(inputBucketName, fileObjectKey));
			GZIPInputStream gzipStream = new GZIPInputStream(s3object.getObjectContent());
			Reader decoder = new InputStreamReader(gzipStream, "ASCII");
			BufferedReader buffered = new BufferedReader(decoder);
			
			String fileLine;
			
			int count = 0;
			
			String header = fileLine = buffered.readLine();
			
			int offset = header.length() + 1;
			
			// TODO: REMOVE AFTER DEBUG!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			
			// TODO: remove count condition to read all file
			while((fileLine = buffered.readLine())!=null /*&& count < 10*/){
				
				// TODO: read line > csv > get offsets > get value > make DataRecord and return list

				String[] fields = fileLine.split(",");
				
				if(DataFileParser.isRecordValid(fields)){
//					System.out.println(offset + "\t:" + fileLine);
//					System.out.println("\t\t\t DBT: " + DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP));
					
//					long recordFromOffset = offset;
//					int recordLength = fileLine.length();
//					sortValue
					
					double dryBulbTemp = Double.parseDouble(DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP));
					
//					System.out.println("=========================================================");
					System.out.println("read record \t\t" + dryBulbTemp);
//					System.out.println("=========================================================");
					
					dataRecordList.add(new DataRecord(fileObjectKey, offset, fileLine.length(), dryBulbTemp));
				}
				offset = offset + fileLine.length() + 1;
				count++;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to read file while converting to DataRecord");
			e.printStackTrace();
		}
		
		return dataRecordList;
	}
	
	public HashMap<Integer, ArrayList<String>> getS3Parts(int parts){
		
		Collections.sort(this.fileNameSizeList_GLOBAL, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				int val1 = Integer.parseInt(o1.split(":")[1]);
				int val2 = Integer.parseInt(o2.split(":")[1]);
				if(val1 > val2) {return 1;}
				if(val1 < val2) {return -1;}
				return 0;
			}
		});
		
		HashMap<Integer, ArrayList<String>> partitionMap = new HashMap<Integer, ArrayList<String>>(); 
	
		int spinner = 0;
		for (String fileNameSize : fileNameSizeList_GLOBAL){
			if(spinner == parts) {spinner = 0;}
			
			if(!partitionMap.containsKey(spinner)){
				partitionMap.put(spinner, new ArrayList<String>());
			}
			partitionMap.get(spinner).add(fileNameSize.split(":")[0].trim());
			
			spinner++;
		}
		
		return partitionMap;
	}


	
	/*
	 * 			WRITE TO EC2 LOCAL DISK
	 * 
	 * */

	public static void writeToEC2(List<DataRecord> drsPartListToBeWritten, int serverNumber_p, int fromServer_s) throws IOException, JSchException, SftpException {
		String fileName = Constants.SAMPLESORT_PART_TEMP_FOLDER+"/s"+fromServer_s+"p"+serverNumber_p+".txt";
			
		System.out.println("FileSystem : writeToEC2 : begin writing to local EC2 : " + fileName);
		
		File f = null;
		f = new File(Constants.SAMPLESORT_PART_TEMP_FOLDER);
		f.mkdir();
		
		System.out.println("f.mkdir for " + fileName);
		
		System.out.println("test reading folders");
		File folderIn1 = new File(Constants.SAMPLESORT_PART_TEMP_FOLDER+"/");
		
		for(File partFile : folderIn1.listFiles()){
			System.out.println("folder: " + partFile.getName());
		}
		
		FileOutputStream fout = new FileOutputStream(fileName);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(drsPartListToBeWritten);

		if(serverNumber_p != fromServer_s){
			writeToLocalOfOtherServer(serverNumber_p, fromServer_s);
		}
		
		oos.close();
		fout.close();
		
		System.out.println("FileSystem : writeToEC2 : done writing all : " + fileName);
	}


	/**
	 * GIVEN	source filename
	 * 			destination file path
	 * 			
	 * Copies source file to destination on other server
	 * */
	private static void writeToLocalOfOtherServer(int serverNumber_p, int fromServer_s) throws SocketException, IOException, JSchException, SftpException {
		
		System.out.println("FileSystem : writeToLocalOfOtherServer : begin : ip " + serverIPaddrMap.get(serverNumber_p));
		System.out.println("FileSystem : src : " + Constants.SAMPLESORT_PART_TEMP_FOLDER + "/s" + fromServer_s + "p" + serverNumber_p 
				+ ".txt" + " destfolder: "	+ Constants.SAMPLESORT_MY_PART);
		
		scpCopy(Constants.SAMPLESORT_PART_TEMP_FOLDER+"/s"+fromServer_s+"p"+serverNumber_p+".txt", 
				Constants.SAMPLESORT_MY_PART,
				serverIPaddrMap.get(serverNumber_p));
		// copy from sample sort parts temp TO sample sort my parts of other server
		
		System.out.println("FileSystem : writeToLocalOfOtherServer : done");
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
		    File localFile = new File(fsrc);
		    System.out.println("local file name : " + fsrc + " localFile " + localFile.getName());
		   
		    //If you want you can change the directory using the following line.
//		    channel.cd("Project");
		    System.out.println("pwd " + channel.pwd());
		    System.out.println("sampleSortPartTemp/"+localFile.getName() + " ::  " + "sampleSortMyParts/");
		    channel.put("sampleSortPartTemp/"+localFile.getName() , "Project/sampleSortMyParts/"+localFile.getName());
		    
		    channel.disconnect();
		session.disconnect();
	}
	
	/*
	 * 			WRITE TO EC2 LOCAL DISK
	 * 
	 * */
	
	public ArrayList<DataRecord> readMyParts(int serverNumber) throws IOException, ClassNotFoundException{
		
		ArrayList<DataRecord> myDataRecordList = new ArrayList<DataRecord>();
		
		File folderIn = new File(Constants.SAMPLESORT_MY_PART_RELATIVE_FOLDER+"/");

		System.out.println("Folder In : " + folderIn);
		
		if(null != folderIn){
			for(File partFile : folderIn.listFiles()){
				if(partFile.getName().contains("p"+serverNumber)){
					System.out.println("Server "+serverNumber + " reading at " + partFile.getName());
					FileInputStream fileStream = new FileInputStream(folderIn+"/"+partFile.getName());
					ObjectInputStream ois = new ObjectInputStream(fileStream);
					
					ArrayList<DataRecord> readList = (ArrayList<DataRecord>) ois.readObject();
					System.out.println("read list " + readList + "");
					// TODO: MAKE MERGER
					myDataRecordList.addAll(readList);
					ois.close();
					fileStream.close();
				}
			}
		}
		return myDataRecordList;
	}

	/**
	 * This method writes the part file, in the format as required 
	 * 			wban #, date, time, and temperature
	 * 
	 * This should be noted when reading this file.
	 * 
	 * 1. The file is first written to local disk. 
	 * 2. Then it is copied to the output S3 bucket.
	 * 3. The temp file is then deleted from local disk.
	 * 
	 * Function used in Server.java at the end of complete sort
	 * */
	public void writePartsToOutputBucket(List<DataRecord> sortedDataRecords, int fromServerNumber) throws IOException {
		try{
			// 1. make temp file
			File tempFileToDelete = new File(Constants.S3_OUTPUT_PART_TEMP_FILE);
			BufferedWriter bufWriter = new BufferedWriter(new FileWriter(tempFileToDelete));
			
			System.out.println("writePartsToOutputBucket enter " + sortedDataRecords.size());
			for(DataRecord drToWrite : sortedDataRecords){
				String[] fields = drToWrite.readRecord(this.inputBucketName).split(",");
				
				String wban = DataFileParser.getValueOf(fields, DataFileParser.Field.WBAN_NUMBER);
				String date = DataFileParser.getValueOf(fields, DataFileParser.Field.YEARMONTHDAY);
				String time = DataFileParser.getValueOf(fields, DataFileParser.Field.TIME);
				String dryBulbTemp = DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP);
				
				String outputInRequiredFormat = wban + ", " + date + ", " + time + ", " + dryBulbTemp + "\n";
				
				System.out.println("writing " + outputInRequiredFormat);
				bufWriter.write(outputInRequiredFormat);
			}
			// 3. close streams and delete temp file
			bufWriter.close();

			System.out.println("writePartsToOutputBucket written to file " + tempFileToDelete.getName());
			// 2. write to S3 bucket
			String serverNum = fromServerNumber+"";
			String fileNameOnS3Bucket = Constants.MyS3BucketOuputPart_DistributedEC2Sort_FOLDER
											+"/part-"+("00000" + serverNum).substring(serverNum.length());
			
			System.out.println("writePartsToOutputBucket moving to s3 " + outputBucketName);
			PutObjectResult result = s3client.putObject(new PutObjectRequest(this.outputBucketName, 
					fileNameOnS3Bucket, tempFileToDelete).withAccessControlList(acl));
			System.out.println("result1 " + result);			
						
			System.out.println("writePartsToOutputBucket moved to s3");

			// later uncomment
//			tempFileToDelete.delete();
		}
		catch(Exception e){
			System.out.println("failure in writePartsToOutputBucket");
			e.printStackTrace();
		}
	}

	
	/**
	 * Read output part files stored in DistributedEC2Sort folder from output S3 bucket
	 * 
	 * 1. find list of part files under the folder DistributedEC2Sort
	 * 2. read the strings
	 * 3. display topX number of data records from that part
	 *  
	 * This function could be called from the Client.java class, after all the
	 * servers have finished the sample sort, and written the parts to a common
	 * S3 bucket folder (using the method writePartsToOutputBucket above)
	 * */
	
	public static void printFinalOutputPartFiles(String outputBucketName, int topX){

		
//		ArrayList<DataRecord> dataRecordList = new ArrayList<DataRecord>();
		
		try {
			
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			ObjectListing objList = s3client.listObjects(outputBucketName);
			
			for(S3ObjectSummary objSum : objList.getObjectSummaries() ){
				if(objSum.getKey().contains(Constants.MyS3BucketOuputPart_DistributedEC2Sort_FOLDER + "part-")){
					System.out.println("FILE: " + objSum.getKey());
					
					String fileToRead = objSum.getKey();
					S3Object partObj = s3client.getObject(new GetObjectRequest(outputBucketName,fileToRead));
					
					BufferedReader partBR = new BufferedReader(new InputStreamReader(partObj.getObjectContent()));
					
					int count = 0;
					String line;
					while(null != (line = partBR.readLine()) && count < topX){
						System.out.println(line);
						count++;
					}
					
					partBR.close();
					partObj.close();
				}
			}
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to read file while converting to DataRecord");
			e.printStackTrace();
		}
		
	}
	
	
	// Getters and Setters
	
	public static HashMap<Integer, String> getServerIPaddrMap() {
		return serverIPaddrMap;
	}

}
