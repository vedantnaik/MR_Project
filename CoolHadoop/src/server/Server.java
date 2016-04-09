
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
	private static int mypart_serversReplied = 0;

//	private static List<Double> dataRecordPivotsList = new ArrayList<Double>(1000);
//	private static List<Double> serverDataRecordPivotValuesList = new ArrayList<Double>();
//	private static List<Double> globalDataRecordPivotValuesList = new ArrayList<Double>();
//	private static List<Double> stage5ReadDataRecordList = new ArrayList<Double>(1000);
	
	private static boolean distributePivotON = false;
	private static boolean globalPivotON = false;

	private static boolean receivingMyPartitionON = false;
	static Object lock;

	// List of Sockets and OutputStream
	private static Map<Integer, DataOutputStream> outDist = null;
	private static Map<Integer, Socket> sendingSocketDist = null;
	private static DataOutputStream outClient = null;
	private static int totalServers;
	
	///// DataRecord sort from s3
	
	private static String inputBucketName;
	private static String outputBucketName;

	private static String inputFolder;
	private static String outputFolder;
	
	private static List<File> fileNameList = new ArrayList<File>(100);
	
	private static Configuration config;
	private static Job job; 

	public Server(Socket newConnection) throws UnknownHostException,
			IOException {
		this.connection = newConnection;

		initOtherSockets();
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
		if (args.length != 5) {
			System.out.println("Syntax error: Include my Number");
			System.out.println("Usage: Server <servernumber> <InputBucketName>");
			System.exit(0);
		}
		inputBucketName = args[1];
		outputBucketName = args[2];
		inputFolder = args[3];
		outputFolder = args[4];
		
		System.out.println("Input bucket: " + inputBucketName);
		System.out.println("Output bucket: " + outputBucketName);
		System.out.println("Input folder: " + inputFolder);
		System.out.println("Output folder: " + outputFolder);
		
//		MRFS = new FileSystem(inputBucketName, outputBucketName, inputFolder, outputFolder);
		config = new Configuration();
		job = Job.getInstance(config);
		lock = new Object();
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

					String[] receivedResult = { "", "" };

					if (received.contains("#")) {
						receivedResult = received.split("#");
					} else {
						receivedResult[0] = received;
					}

					if (receivedResult[0].equals(Constants.MAPFILES)) {
						
						if(receivedResult[1].contains(Constants.START)){
							System.out.println("STAGE 1 start");
							receivingMyPartitionON = true;
						}
						
						if(receivedResult[1].contains(Constants.END)){
							System.out.println("STAGE 1 "
									+ "receiving files ends");							
							
							receivingMyPartitionON = false;
							outClient = out;
							out.writeBytes(Constants.STARTING_MAP+"\n");
							System.out.println("replied " + Constants.STARTING_MAP + " to client");
						}

					} else if (receivedResult[0].equals(Constants.MAP)) {

						// select pivots
						System.out.println("STAGE 2 : "
								+ "Selecting my pivots");
						stage1_start_map_phase();

					} else if (receivedResult[0].equals("kill")) {
						System.out.println("KILLED!");
						System.exit(0);
					} else {
						if(receivingMyPartitionON){
							
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
		} catch (Exception e) {
			System.out.println("Thread cannot serve connection");
			e.printStackTrace();
		}
	}

	
	/**
	 * Calls the MapperHandler as thread
	 * */
	private void stage1_start_map_phase() {
		System.out.println("Stage1: read input files assigned to server...");

		// 1. Calling Mapper Handler thread
		System.out.println("Mapper Starts");
		
		MapperHandler mh = new MapperHandler(fileNameList, job);
		mh.run();
	
		System.out.println("Mapper Ends");
	}


}
