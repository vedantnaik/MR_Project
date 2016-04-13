package master;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.Constants;
import coolmapreduce.Configuration;
import coolmapreduce.Job;

/**
 * A sample Master which connects to the slave server and can issue a 
 * sorting command with the contents of a string. It can also kill 
 * the server slaves and self destruct itself!
 *
 */
public class Master {

	static int port = 1210;

	private static Map<Integer, DataOutputStream> out = null;
	private static Map<Integer, Socket> sendingSocket = null;
	private static Map<Integer, BufferedReader> inFromServer = null;
	private static int totalServers;
	private String masterServer = null;
	
	private static Map<Integer, String> servers;
	private static boolean localServersFlag = false;

	public Master(boolean _localFlag) {
		try {
			localServersFlag = _localFlag;
			servers = Configuration.getServerIPaddrMap();
			totalServers = servers.size();
			out = new HashMap<>(2 * totalServers);
			sendingSocket = new HashMap<>(2 * totalServers);
			inFromServer = new HashMap<>(2 * totalServers);
			System.out.println("servers are " + servers);
			masterServer = null;

			initServerSockets();

		} catch (IOException e) {
			System.out.println("Unable to connect to all servers! "
					+ masterServer);

			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public Master(){
		this(true);
	}

	public void initServerSockets()
			throws UnknownHostException, IOException {
		System.out.println("Running LOCAL " + localServersFlag);
		// TODO: No more master
		masterServer = servers.get(0);
		for (int i = 0; i < totalServers; i++) {
			if (localServersFlag) {
				System.out.println("Connecting to localhost " + (port + i));
				sendingSocket.put(i, new Socket("localhost", (port + i)));
			} else {
				System.out.println("Connecting to " + servers.get(i) +
						"@" + (port + i));
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
	
	// TODO: Should return boolean

	public void startJob(Job job, String inputBucketName,
			String outputBucketName, String inputFolder, String outputFolder)
			throws IOException {

		// FileSystem myS3FS = new FileSystem(inputBucketName, outputBucketName,
		// inputFolder, outputFolder);

		// TODO: remove MapperTester
		Map<Integer, List<String>> partsMap = mimicMyParts();

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

	// sends a particular phase to start to all servers
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

	// It then waits for the Server to reply back when their phase
	// is done.
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
	 */
	private void killer() throws UnknownHostException,
			IOException {
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
				out.writeBytes(Constants.KILL+ "#" + "\n");
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
	public void jobConfigurer(String args[]) throws Exception {
		if (args.length < 4) {
			System.out
					.println("Usage Client <inputBucketname> <outputBucketName>"
							+ "<inputFolder> <outputFolder> <LOCAL/nothing>");
			System.out.println("Client some some some some LOCAL");
			System.out.println("or");
			System.out.println("Client some some some some");
			System.exit(0);
		}
		// needs to come from FileInputPaths
		String inputBucketName = args[0];
		String outputBucketName = args[1];
		String inputFolder = args[2];
		String outputFolder = args[3];
		if (args.length > 4 && args[4].equals(Constants.LOCAL))
			localServersFlag = true;

		System.out.println("Input bucket: " + inputBucketName);
		System.out.println("Output bucket: " + outputBucketName);
		System.out.println("Input folder: " + inputFolder);
		System.out.println("Output folder: " + outputFolder);
		System.out.println("Running Local ? " + localServersFlag);
		System.out.println("Reading s3 bucket");
		// MRFS = new FileSystem(inputBucketName, outputBucketName, inputFolder,
		// outputFolder);
		
		// TODO: later change
		System.out.println("reading config");
		config = new Configuration();
		
		
		System.out.println("connecting to servers");
		Master master = new Master();
		System.out.println("Connected to all Servers!");
		System.out.println("Informing servers to begin!");
		
		
		startJob(config, inputBucketName, outputBucketName, inputFolder,
				outputFolder);
		Thread.sleep(1000000000);
	}
	*/

	public static String PATH = "C://Users//Dixit_Patel//Google Drive//Working on a dream//StartStudying//sem4//MapReduce//homeworks//hw8-Distributed Sorting//MR_Project//CoolHadoop//resources";

	public static List<String> getStupidFiles() {
		List<String> files = new ArrayList<>();
		files.add("alice.txt.gz");

		for (int i = 0; i < 3; i++)
			files.add("alice.txt.gz");

		return files;
	}

	public static Map<Integer, List<String>> mimicMyParts() {
		Map<Integer, List<String>> parts = new HashMap<>();
		for (int i = 0; i < 3; i++)
			parts.put(i, getStupidFiles());

		return parts;
	}
}
