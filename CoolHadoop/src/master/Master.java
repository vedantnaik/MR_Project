package master;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.Constants;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import coolmapreduce.Configuration;
import coolmapreduce.Job;

/**
 * A Master which controls the slave Servers and can issue commands It can also
 * kill the server slaves and self destruct itself!
 */
public class Master {

	static int port = 1210;

	private static Map<Integer, DataOutputStream> out = null;
	private static Map<Integer, Socket> sendingSocket = null;
	private static Map<Integer, BufferedReader> inFromServer = null;
	private static int totalServers;

	private static Map<Integer, String> servers;
	private static boolean localServersFlag = false;

	/**
	 * Init the DataOutputStream, Socket and BufferedReader according to the
	 * flag specified in the Constructor. If local flag is set, the localhost is
	 * used and the port numbers are incremented according to their server
	 * number to provide different local-ports. If not, the DNS files of EC2
	 * instances are used and started at port 1210
	 * 
	 * @param config
	 * 				the configuration config object of the Job
	 * @param _localFlag
	 *            boolean value telling if the slave servers are local or not
	 */
	public Master(Configuration config, boolean _localFlag) {
		try {
			localServersFlag = _localFlag;
			servers = config.getServerIPaddrMap();
			totalServers = servers.size();
			out = new HashMap<>(2 * totalServers);
			sendingSocket = new HashMap<>(2 * totalServers);
			inFromServer = new HashMap<>(2 * totalServers);
			System.out.println("servers are " + servers);
			System.out.println("Configured Local? " + localServersFlag);
			initServerSockets();

		} catch (IOException e) {
			System.out.println("Unable to connect to all servers! ");

			e.printStackTrace();
			System.exit(0);
		}
	}

	/**
	 * By default sets it local Server and calls the parameterized Constructor.
	 */
	public Master(Configuration config) {
		this(config, true);
	}

	/**
	 * Init the DataOutputStream, Socket and BufferedReader in a HashMap
	 * according to the server-number for easy access for upcoming
	 * commmunication between Master and Slave Servers
	 * 
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void initServerSockets() throws UnknownHostException, IOException {
//		System.out.println("Running LOCAL " + localServersFlag);
		
		for (int i = 0; i < totalServers; i++) {
			if (localServersFlag) {
				System.out.println("Connecting to localhost " + (port + i));
				sendingSocket.put(i, new Socket("localhost", (port + i)));
			} else {
				System.out.println("Connecting to " + servers.get(i) + "@"
						+ (port + i));
				sendingSocket.put(i, new Socket(servers.get(i), port));
			}

			Socket socketTmp = sendingSocket.get(i);
			socketTmp.setSoTimeout(0);
			sendingSocket.put(i, socketTmp);
			out.put(i, new DataOutputStream(sendingSocket.get(i)
					.getOutputStream()));
			inFromServer.put(i, new BufferedReader(new InputStreamReader(
					sendingSocket.get(i).getInputStream())));

		}
	}

	/**
	 * Starts the Job on the slave instances by communicating them with their
	 * Protocols defined. It starts as follows 
	 * <p><ul>
	 * <li> Read the serialized job from
	 * the jobfile. Wait for a reply till all servers have replied JOBREAD 
	 * <li> 1. Send the file names to the slaves about which files to read and
	 * communicate a end of transmission. 
	 * <li> 2. Wait for an ACK saying FILES_READ
	 * <li> 3. Send instruction to start reading the files. 
	 * <li> 4. Wait for MAP_FINISH instruction from slaves. 
	 * <li> 5. Send a SHUFFLEANDSORT instruction to slaves
	 * <li> 6. Wait for an ACK for SHUFFLEFINISH 
	 * <li> 7. Send instruction to start REDUCE phase 
	 * <li> 8. Wait for REDUCEFINISH from all servers.
	 * </ul><p>
	 * 
	 * Lastly send a KILL instruction to stop the JVM
	 * 
	 * @param job
	 *            the job instance to work on
	 * @param inputBucketName
	 *            the input bucket name
	 * @param outputBucketName
	 *            the output bucket name
	 * @param inputFolder
	 *            the input folder inside bucket
	 * @param outputFolder
	 *            the output folder inside bucket
	 * @throws IOException
	 */
	public void startJob(Job job, String inputBucketName,
			String outputBucketName, String inputFolder, String outputFolder)
			throws IOException {
		// TODO: Should return boolean


		// TODO: remove mimicMyParts
		Map<Integer, List<String>> partsMap = getS3Parts(inputBucketName, inputFolder, totalServers);

		try {

			// 0. Serialize Job
			sendAConstant(Constants.READJOB, job.getJobName());

			// wait till job is read
			waitForReply(Constants.JOBREAD);

			// 1. Sending files to server
			for (int i = 0; i < totalServers; i++) {

				out.get(i).writeBytes(
						Constants.MAPFILES + "#" + Constants.START + "\n");
				System.out.println("Map Parts to " + servers.get(i) + " => "
						+ partsMap.get(i));
				for (String fileNameIter : partsMap.get(i)) {
					out.get(i).writeBytes(fileNameIter + "\n");
					Thread.sleep(1000);
				}

				out.get(i).writeBytes(
						Constants.MAPFILES + "#" + Constants.END + "\n");
				System.out.println("Files sent to Server ..." + i);
			}

			// 2. wait for FILES_READ#Number from all servers
			waitForReply(Constants.FILES_READ);

			// 3. Send MAP#START to all servers
			sendAConstant(Constants.MAP, Constants.START);

			// 4. wait for MAPFINISH#Number from all servers
			// or replies MAPFAILURE#Number
			waitForReply(Constants.MAP_FINISH);
			// TODO: What todo if FAILURE? restructure fun
			// to accomodate failure and restart on some node later

			// 5. Send SHUFFLEANDSORT to all servers
			sendAConstant(Constants.SHUFFLEANDSORT, Constants.START);

			// 6. Wait for SHUFFLEANDSORT to finish
			waitForReply(Constants.SHUFFLEFINISH);

			// 7. Send REDUCE to all servers
			sendAConstant(Constants.REDUCE, Constants.START);

			// 8. Wait for REDUCEFINISH to finish
			waitForReply(Constants.REDUCEFINISH);

			System.out.println("Waiting for Servers to finish");

			System.out.println("Stopping Servers");

			killer();

		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
			e.printStackTrace();
		}
	}

	/**
	 * Generalized function which sends a particular phase to to all servers
	 * 
	 * @param _constant1
	 *            the constant to begin
	 * @param _beginEnd
	 *            the begin keyword
	 * @throws IOException
	 */
	private void sendAConstant(String _constant1, String _beginEnd)
			throws IOException {
		System.out
				.println("sending command to " + _constant1 + " " + _beginEnd);
		// distribute pivots
		for (int i = 0; i < totalServers; i++) {
			out.get(i).writeBytes(_constant1 + "#" + _beginEnd + "\n");
		}
		System.out.println("Sent command for " + _constant1 + " " + _beginEnd
				+ "!");
	}

	/**
	 * waits for all the slave Servers to reply back when their phase is done.
	 * It accounts for each reply from all the Servers
	 * 
	 * @param waitForConstant
	 *            the constant to wait for
	 * @throws IOException
	 */
	private void waitForReply(String waitForConstant) throws IOException {
		System.out.println("wait for reply " + waitForConstant);
		int replies = 0;
		boolean[] replied = new boolean[totalServers];
		while (true) {
			for (int i = 0; i < replied.length && !replied[i]; i++) {

				String result = inFromServer.get(i).readLine();
				System.out.println("received " + result);

				String[] returnedResult = result.split("#");

				if (returnedResult[0].equals(waitForConstant)) {
					System.out.println(returnedResult[0] + "" + " from : " + i);

					replied[i] = true;
					if (replied[i]) {
						replies++;
					}
				}
			}
			if (replies == totalServers) {
				System.out.println("replied by " + replies);
				break;
			}

		}
		// totalServers replied
		System.out.println("finished waiting for reply " + waitForConstant
				+ Arrays.toString(replied));
	}

	/**
	 * Sends a kill command to server to terminate itself
	 * 
	 * @param serverIP
	 *            the server ip of the server eg. 127.0.0.1
	 * @param serverPort
	 *            the server port eg. 1212
	 * 
	 */
	private void killer() throws UnknownHostException, IOException {
		try {
			for (int i = 0; i < totalServers; i++) {
				Socket sendingSocket = null;
				if (localServersFlag) {
					sendingSocket = new Socket("localhost", (port + i));
				} else {
					sendingSocket = new Socket(servers.get(i), port);
				}

				DataOutputStream out = new DataOutputStream(
						sendingSocket.getOutputStream());
				out.writeBytes(Constants.KILL + "#" + "\n");
				out.close();
				sendingSocket.close();
			}
		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
		}
	}

	/**
	 * main sort driver program
	 * 
	 * @param args
	 * @throws Exception
	 */
	/*
	 * public void jobConfigurer(String args[]) throws Exception { if
	 * (args.length < 4) { System.out
	 * .println("Usage Client <inputBucketname> <outputBucketName>" +
	 * "<inputFolder> <outputFolder> <LOCAL/nothing>");
	 * System.out.println("Client some some some some LOCAL");
	 * System.out.println("or");
	 * System.out.println("Client some some some some"); System.exit(0); } //
	 * needs to come from FileInputPaths String inputBucketName = args[0];
	 * String outputBucketName = args[1]; String inputFolder = args[2]; String
	 * outputFolder = args[3]; if (args.length > 4 &&
	 * args[4].equals(Constants.LOCAL)) localServersFlag = true;
	 * 
	 * System.out.println("Input bucket: " + inputBucketName);
	 * System.out.println("Output bucket: " + outputBucketName);
	 * System.out.println("Input folder: " + inputFolder);
	 * System.out.println("Output folder: " + outputFolder);
	 * System.out.println("Running Local ? " + localServersFlag);
	 * System.out.println("Reading s3 bucket"); // MRFS = new
	 * FileSystem(inputBucketName, outputBucketName, inputFolder, //
	 * outputFolder);
	 * 
	 * // TODO: later change System.out.println("reading config"); config = new
	 * Configuration();
	 * 
	 * 
	 * System.out.println("connecting to servers"); Master master = new
	 * Master(); System.out.println("Connected to all Servers!");
	 * System.out.println("Informing servers to begin!");
	 * 
	 * 
	 * startJob(config, inputBucketName, outputBucketName, inputFolder,
	 * outputFolder); Thread.sleep(1000000000); }
	 */
//
//	public static String PATH = "C://Users//Dixit_Patel//Google Drive//Working on a dream//StartStudying//sem4//MapReduce//homeworks//hw8-Distributed Sorting//MR_Project//CoolHadoop//resources";
//
//	public static List<String> getStupidFiles() {
//		List<String> files = new ArrayList<>();
//		files.add("alice.txt.gz");
//
//		for (int i = 0; i < 3; i++)
//			files.add("alice.txt.gz");
//
//		return files;
//	}
//
//	public static Map<Integer, List<String>> mimicMyParts() {
//		Map<Integer, List<String>> parts = new HashMap<>();
//		for (int i = 0; i < 3; i++)
//			parts.put(i, getStupidFiles());
//
//		return parts;
//	}
	
	public List<String> getObjectSummariesForBucket(String inputBucketName, String inputFolder){
		List<String> objectSummaries = new ArrayList<>();
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		
		ObjectListing objList = s3client.listObjects(inputBucketName);

		for(S3ObjectSummary objSum : objList.getObjectSummaries() ){
			if(objSum.getKey().contains(inputFolder)){
				objectSummaries.add(objSum.getKey() + ":" + objSum.getSize());
			}
		}
		
		return objectSummaries;
	}
	
	
	/**
	 * Divide the files among servers such that every server handles roughly same amount of data
	 * */
	public Map<Integer, List<String>> getS3Parts(String inputBucketName, String inputFolder, int parts){
		List<String> objectSummaries = getObjectSummariesForBucket(inputBucketName, inputFolder);
		Collections.sort((objectSummaries), new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				int val1 = Integer.parseInt(o1.split(":")[1]);
				int val2 = Integer.parseInt(o2.split(":")[1]);
				if(val1 > val2) {return 1;}
				if(val1 < val2) {return -1;}
				return 0;
			}
		});
		
		Map<Integer, List<String>> partitionMap = new HashMap<>(); 
	
		int spinner = 0;
		for (String fileNameSize : objectSummaries){
			if(spinner == parts) {spinner = 0;}
			
			if(!partitionMap.containsKey(spinner)){
				partitionMap.put(spinner, new ArrayList<String>());
			}
			partitionMap.get(spinner).add(fileNameSize.split(":")[0].trim());
			
			spinner++;
		}
		
		return partitionMap;
	}
	
	
}
