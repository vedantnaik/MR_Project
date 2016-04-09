
package server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.Constants;
import coolmapreduce.Configuration;
import coolmapreduce.Job;
import coolmapreduce.MapperHandler;

/**
 * Server program which listens on the port requested or passed in the URL.
 * Contains two functions, sort and kill(itself)
 */
public class Server implements Runnable {
	private Socket connection = null;
	static ServerSocket serverSocket = null;

	static int port = 1210;
	static int numberOfProcessors = 0;
	static int serverNumber = 0;

	private static int serversReplied = 0;
	private static boolean[] replied;
	
	private static int mypart_serversReplied = 0;

//	private static List<Double> dataRecordPivotsList = new ArrayList<Double>(1000);
//	private static List<Double> serverDataRecordPivotValuesList = new ArrayList<Double>();
//	private static List<Double> globalDataRecordPivotValuesList = new ArrayList<Double>();
//	private static List<Double> stage5ReadDataRecordList = new ArrayList<Double>(1000);
	
	private static boolean distributePivotON = false;
	private static boolean globalPivotON = false;

	private static boolean receivingMapFiles = false;
	static Object lock;

	// List of Sockets and OutputStream
	private static Map<Integer, DataOutputStream> outDist = null;
	private static Map<Integer, Socket> sendingSocketDist = null;
	private static DataOutputStream outClient = null;
	private static int totalServers;

	
	private static String inputBucketName;
	private static String outputBucketName;

	private static String inputFolder;
	private static String outputFolder;
	
	private static List<File> fileNameList = new ArrayList<File>(100);
	
	private static Configuration config;
	private static Job job;
	private static String localServers;

	public Server(Socket newConnection) throws UnknownHostException,
			IOException {
		this.connection = newConnection;
		
		if(localServers != null && localServers.equals(Constants.LOCAL))
			initOtherSockets();
		else
			initLocalOtherSockets();
	}

	public void initLocalOtherSockets() throws UnknownHostException,
			IOException {
		for (int i = 0; i < totalServers; i++) {
			if (outDist.get(i) == null) {
				sendingSocketDist.put(i, new Socket("localhost", port + serverNumber));
				outDist.put(i, new DataOutputStream(
						sendingSocketDist.get(i).getOutputStream()));
			}
		}
		
	}

	public void initOtherSockets() throws UnknownHostException, IOException {
		for (int i = 0; i < totalServers; i++) {
			if (outDist.get(i) == null) {
				sendingSocketDist.put(i, new Socket(Configuration.getServerIPaddrMap().get(i), port));
				outDist.put(i, new DataOutputStream(
						sendingSocketDist.get(i).getOutputStream()));
			}
		}
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 6) {
			System.out.println("Syntax error: Include my Number");
			System.out.println("Usage: Server <servernumber> <InputBucketName>");
			System.exit(0);
		}
		inputBucketName = args[1];
		outputBucketName = args[2];
		inputFolder = args[3];
		outputFolder = args[4];
		localServers = args[5];
		
		System.out.println("Input bucket: " + inputBucketName);
		System.out.println("Output bucket: " + outputBucketName);
		System.out.println("Input folder: " + inputFolder);
		System.out.println("Output folder: " + outputFolder);
		
//		MRFS = new FileSystem(inputBucketName, outputBucketName, inputFolder, outputFolder);
		config = new Configuration();
		job = Job.getInstance(config);
		lock = new Object();
		replied = new boolean[totalServers];
		serverNumber = Integer.parseInt(args[0]);

		totalServers = Configuration.getServerIPaddrMap().size();
		numberOfProcessors = totalServers;
		System.out.println("totalServers " + Configuration.getServerIPaddrMap().size());
		outDist = new HashMap<>(2 * totalServers);
		sendingSocketDist = new HashMap<>(2 * totalServers);
		
		System.out.println("servers to connect to : " + Configuration.getServerIPaddrMap());
		System.out.println("Started Server " + serverNumber + " @ " + port);
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			System.out.println("Death on port " + port + " Try some other port");
			System.exit(0);
		}

		while (true) {
			Socket newCon = serverSocket.accept();
			System.out.println("New connection Startin ...");
			Runnable runnable = new Server(newCon);
			Thread thread = new Thread(runnable);			
			thread.start(); // start new thread to accept each connection
		}
	}

	/**
	 * starts a thread which does the server stuff - sort the input requested
	 * from client - kill itself if client says so.
	 */
	public void run() {
		try {
			BufferedReader inFromClient = new BufferedReader(
					new InputStreamReader(connection.getInputStream()));

			while (true) {
				String received = inFromClient.readLine();
				synchronized (lock) {
					if(null != received){
					DataOutputStream out = new DataOutputStream(
							connection.getOutputStream());

					String[] receivedResult = splitReceived(received);

					if (receivedResult[0].equals(Constants.MAPFILES)) {
						System.out.println("STAGE 1");											
						start_map_files_phase(receivedResult, out);

					} else if(receivedResult[0].equals(Constants.FILES_READ)){
						System.out.println("STAGE 2");
						wait_for_map_files(receivedResult);
						
					}else if (receivedResult[0].equals(Constants.MAP)) {
						System.out.println("STAGE 3 Map phase");
						start_map_phase();

					}else if(receivedResult[0].equals(Constants.MAPFAILURE)){
						int whoFailed = Integer.parseInt(receivedResult[1]);
						System.out.println("received failure from "+ whoFailed);
						
						// TODO: should I restart on same server or other server?
					}else if (receivedResult[0].equals("kill")) {
						System.out.println("KILLED!");
						System.exit(0);
					} else {
						if(receivingMapFiles){
							
							// add received filename to list
							
							// TODO: new File to function
							// readInputStringsFromLocalInputBucket(...)
							fileNameList.add(new File(receivedResult[0]));
						}
					}
					lock.notifyAll();
				}
				}
			}
		} catch (IOException e) {
			System.out.println("Thread cannot serve connection, Error"
					+ "in Sending Data via Sockets");
			e.printStackTrace();
		}
	}

	private String[] splitReceived(String received){
		String[] receivedResult = new String[2];
		if (received.contains("#")) {
			receivedResult = received.split("#");
		} else {
			receivedResult[0] = received;
		}
		return receivedResult;
	}
	
	private void start_map_files_phase(String[] receivedResult,
			DataOutputStream out) {

		try {
			if (receivedResult[1].contains(Constants.START)) {
				System.out.println("STAGE 1 start");
				receivingMapFiles = true;
			}

			if (receivedResult[1].contains(Constants.END)) {
				System.out.println("STAGE 1 " + "receiving files ends");

				receivingMapFiles = false;
				outClient = out;
				out.writeBytes(Constants.NEED_TO_STARTING_MAP + "\n");
				System.out.println("replied " + Constants.NEED_TO_STARTING_MAP
						+ " to client");

				outDist.get(0).writeBytes(
						Constants.FILES_READ + "#" + serverNumber + "\n");
			}
		} catch (IOException e) {
			System.out.println("Error in Sending Data via Sockets");
			e.printStackTrace();
		}

	}

	private void wait_for_map_files(String[] receivedResult) {
		try {
			if (serverNumber == 0) {
				int whoRepliedNumber = Integer.parseInt(receivedResult[1]);
				System.out.println("Server completed " + whoRepliedNumber + 
						" " + Constants.FILES_READ);
				
				replied[whoRepliedNumber] = true;
				serversReplied++;
				
				if (serversReplied == totalServers) {
					// send a MAP instruction to start
					for (int i = 0; i < totalServers; i++)
						outDist.get(i).writeBytes(
								Constants.MAP + "#" + i + "\n");
				}
			}
		} catch (Exception e) {
			System.out.println("Exception in wait_for_map_files");
			e.printStackTrace();
		}
	}

	/**
	 * Calls the MapperHandler as thread
	 * @throws IOException 
	 */
	private void start_map_phase() throws IOException {
		System.out.println("Stage1: read input files assigned to server...");

		// 1. Calling Mapper Handler thread
		System.out.println("Mapper Starts");
		
		try {
			// later split and start multiple mappers with threads
			MapperHandler mh = new MapperHandler(fileNameList, job);
			mh.run();
			mh.wait();

		} catch (InterruptedException e) {
			// send to server - 0 only
			outDist.get(0).writeBytes(
						Constants.MAPFAILURE + "#" + serverNumber + "\n");		
			e.printStackTrace();
		}
		System.out.println("Mapper Ends");
	}


}
