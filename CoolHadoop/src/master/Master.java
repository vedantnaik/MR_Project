package master;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
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
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import coolmapreduce.Configuration;
import coolmapreduce.Job;
import fs.FileSys;
import fs.shuffler.LoadDistributor;

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
	
	public Master(){
		// noop
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
						+ (port));
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

		FileSys.makeFolderStructureOnMaster(job.getJobName());
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
				
				// TODO: Null check
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
			
			// Send master MKM back to all slave servers
			merge_mkms_and_send_mastermkm_back(job.getJobName());

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

	private void merge_mkms_and_send_mastermkm_back(String jobName) throws FileNotFoundException, ClassNotFoundException, 
				IOException, JSchException, SftpException {
		// TODO Auto-generated method stub
		//hashmap
		
		// each file in masterMKM
		// merge func call
		// set call / union 
		// 
		
		// {haskeys : originalkeys}
		Map<String, Object> allMasterMKMs = readAllMKMs(jobName);		
		
		// {hashkeys : servernumbers}
		Map<String, Object>  broadcastKeyServerMap = LoadDistributor.getLoadDistribBroadcast(allMasterMKMs, totalServers);	
		
		String masterBroadcastKeyServerMapPath = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER_LOCAL
				.replace("<JOBNAME>", jobName) + Constants.UNIX_FILE_SEPARATOR + Constants.BROADCAST_KEY_SERVER_MAP;
				
		String masterBroadcastMKMPath = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER_LOCAL
				.replace("<JOBNAME>", jobName) + Constants.UNIX_FILE_SEPARATOR + Constants.BROADCAST_MKM_MAP;
		
		File fileDelete = new File(masterBroadcastMKMPath);
		File fileDeleteKS = new File(masterBroadcastKeyServerMapPath);
		fileDelete.delete();
		fileDeleteKS.delete();
		
		FileSys.writeObjectToFile(allMasterMKMs, masterBroadcastMKMPath);
		FileSys.writeObjectToFile(broadcastKeyServerMap, masterBroadcastKeyServerMapPath);
		
		String fdest = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER.replace("<JOBNAME>", jobName) + Constants.BROADCAST_MKM_MAP;
		String fsrc = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER.replace("<JOBNAME>", jobName) + Constants.BROADCAST_MKM_MAP;
		
		String fdestKS = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER.replace("<JOBNAME>", jobName) + Constants.BROADCAST_KEY_SERVER_MAP;
		String fsrcKS = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER.replace("<JOBNAME>", jobName) + Constants.BROADCAST_KEY_SERVER_MAP;
		
		System.out.println("\tMKM " + allMasterMKMs);
		System.out.println("\tKeyServer Map " + broadcastKeyServerMap);
		
		
		for (int i = 0; i < totalServers; i++) {
			System.out.println("Moving MKMs from " + fsrc + "  to " + servers.get(i) + " @ "
					+ fdest);
			FileSys.scpCopy(fsrc, fdest, servers.get(i));
			FileSys.scpCopy(fsrcKS, fdestKS, servers.get(i));
		}		
		
	}
	
	public Map<String, Object> readAllMKMs(String jobName) throws FileNotFoundException, IOException, ClassNotFoundException{
		
		String mapperOutputFolderStr = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER
												.replace("<JOBNAME>", jobName);
		System.out.println("mapfolder to read MKM " + mapperOutputFolderStr);
		File mapperOutputFolder = new File(mapperOutputFolderStr);
		
		Map<String, Object> mapReadFromMKMs = new HashMap<>();
		
		Map<String, Object> allMKMs = new HashMap<>();
		
		for(String f : mapperOutputFolder.list()){
		
			ObjectInputStream iis = new ObjectInputStream(new FileInputStream(
					new File(mapperOutputFolder+"/"+f)));
			mapReadFromMKMs = (HashMap<String, Object>) iis.readObject();
			iis.close();
			System.out.println("Adding size of " + mapReadFromMKMs.size());
			allMKMs.putAll(mapReadFromMKMs);
		}
		
		System.out.println("All MKM's " + allMKMs.size());
		return allMKMs;
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
				if(result == null){
					System.err.println("Did someone die? # " + i);
					replies++;
					replied[i] = true;
					continue;
				}

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
	 * Does a listing of files present in a bucket inside that particular object(inputFolder)
	 * @param inputBucketName the name of the bucket
	 * @param inputFolder the folder(object) inside the bucket to do listing inside
	 * @return List<String> the summaries - like the ls command in UNIX
	 */
	public List<String> getObjectSummariesForBucket(String inputBucketName, String inputFolder){
		List<String> objectSummaries = new ArrayList<>();
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		
		ObjectListing objList = s3client.listObjects(inputBucketName);

		for(S3ObjectSummary objSum : objList.getObjectSummaries() ){
			if(objSum.getKey().contains(inputFolder) && objSum.getKey().length() > inputFolder.length() + 2){
				objectSummaries.add(objSum.getKey() + ":" + objSum.getSize());
			}
		}
		
		return objectSummaries;
	}
	
	/**
	 * Divide the files among servers such that every server handles roughly same amount of data
	 * @param inputBucketName the input bucket read from the Job
	 * @param inputFolder the folder inside the bucket
	 * @param parts the number of servers/the number of parts required to be distributed
	 * @return Map<Integer, List<String>> where integers are the slave server numbers and 
	 * List<String> is the files which belong to a particular slave instance
	 */
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
