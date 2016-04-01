package server;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.FileSystem;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import datafile.DataRecord;
import sorter.LocalFSSorter;

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

	private static List<Double> dataRecordPivotsList = new ArrayList<Double>(1000);
	private static List<Double> serverDataRecordPivotValuesList = new ArrayList<Double>();
	private static List<Double> globalDataRecordPivotValuesList = new ArrayList<Double>();
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
	
	private static List<String> fileNameList = new ArrayList<String>(1000);
	
	private static List<DataRecord> serverDataRecords = new ArrayList<DataRecord>(1000);
	private static List<DataRecord> serverDataRecordsCache = new ArrayList<DataRecord>(1000);
	
	///// File system for this EC2 instance
	
	static FileSystem MRFS;
	
	static final String MYPARTS_SORTED_COMPLETE_FILE = "MYPARTS_SORTED_COMPLETE";
	/////
	

	public Server(Socket newConnection) throws UnknownHostException,
			IOException {
		this.connection = newConnection;

		initOtherSockets();
	}

	public void initOtherSockets() throws UnknownHostException, IOException {
		for (int i = 0; i < totalServers; i++) {
			if (outDist.get(i) == null) {
				sendingSocketDist.put(i, new Socket(FileSystem.getServerIPaddrMap().get(i), port));
				outDist.put(i, new DataOutputStream(
						sendingSocketDist.get(i).getOutputStream()));
			}
		}
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 3) {
			System.out.println("Syntax error: Include my Number");
			System.out.println("Usage: Server <servernumber> <InputBucketName>");
			System.exit(0);
		}
		inputBucketName = args[1];
		outputBucketName = args[2];
		MRFS = new FileSystem(inputBucketName, outputBucketName);
	
		lock = new Object();
		serverNumber = Integer.parseInt(args[0]);

		totalServers = FileSystem.getServerIPaddrMap().size();
		numberOfProcessors = totalServers;
		System.out.println("totalServers " + FileSystem.getServerIPaddrMap().size());
		outDist = new HashMap<>(2 * totalServers);
		sendingSocketDist = new HashMap<>(2 * totalServers);
		
		System.out.println("servers to connect to : " + FileSystem.getServerIPaddrMap());
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
	public synchronized void run() {
		try {
			BufferedReader inFromClient = new BufferedReader(
					new InputStreamReader(connection.getInputStream()));

			while (true) {
				String received = inFromClient.readLine();
				synchronized (lock) {
					if(null != received){
					DataOutputStream out = new DataOutputStream(
							connection.getOutputStream());
//					System.out.println("received " + received);
					String[] receivedResult = { "", "" };

					if (received.contains("#")) {
						receivedResult = received.split("#");
					} else {
						receivedResult[0] = received;
					}

					if (receivedResult[0].equals("sort")) {
						
						if(receivedResult[1].contains("start")){
							System.out.println("STAGE 1 start");
							receivingMyPartitionON = true;
						}
						
						if(receivedResult[1].contains("end")){
							System.out.println("STAGE 1 "
									+ "receiving ends, sorting my data");
							
							stage1_sort_my_partition();
							receivingMyPartitionON = false;
							outClient = out;
							out.writeBytes("done#\n");
							System.out.println("replied done# to client");
						}

					} else if (receivedResult[0].equals("distribute")) {

						// select pivots
						System.out.println("STAGE 2 : "
								+ "Selecting my pivots");
						stage2_select_my_pivots();

					} else if (receivedResult[0].equals("distributePivot")) {
						// distribute the selected pivots
						System.out.println("STAGE 3: "
								+ "Receive Distributed Pivots");
						
						stage3_distribute_pivots(receivedResult);

					} else if (receivedResult[0].equals("globalpivot")) {

						// selecting and sending out gloabl pivots
						System.out.println("STAGE4 : "
								+ "Global Pivot");
						stage4_global_pivots(receivedResult);

					} else if (receivedResult[0].equals("mypart")) {
						// specific part receiving phase
						System.out.println("STAGE 5 : "
								+ "mypart receiving stage");
						stage5_mypart_receive_parts(receivedResult, outClient);
						
					} else if (receivedResult[0].equals("kill")) {
						System.out.println("KILLED!");
						System.exit(0);
					} else {
						if(receivingMyPartitionON){
							// add received filename to list
							fileNameList.add(receivedResult[0]);
						}
						else if (distributePivotON) {
							System.out.println("adding to server data records pivot list " + receivedResult[0]);
							serverDataRecordPivotValuesList.add(Double.parseDouble((receivedResult[0])));
						} else if (globalPivotON) {
							System.out.println("adding to global pivot list " + receivedResult[0]);
							globalDataRecordPivotValuesList.add(Double.parseDouble(receivedResult[0]));
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
	 * 1. Read ALL DataRecords from input files assigned to this server
	 * 2. Sort ALL DataRecords
	 * */
	private void stage1_sort_my_partition() {
		System.out.println("Stage1: read input files assigned to server...");
		// TODO: later make this in threads
		int count = 0;
		// 1. Read
		for (String fileName : fileNameList){
			try {
				System.out.println("\tReading file " + fileName + " " + count);
				serverDataRecords.addAll(FileSystem.readInputDataRecordsFromInputBucket(inputBucketName, fileName));
				count++;
			} catch (Exception e) {
				System.err.println("SERVER : Stage 1 Sorting : unable to read file");
				e.printStackTrace();
			}
		}
		
		// 2. Sort
		System.out.println("Sorting stage1");
		Collections.sort(serverDataRecords);
		System.out.println("Sorted stage1");
	}

	/**
	 * 1. Calculate pivots for data records in this server
	 * 2. Send calculated pivots from this server to server-0 (master)
	 * */
	private void stage2_select_my_pivots() throws IOException {
		
		String piv = "";
		List<Double> pivArray = new ArrayList<Double>(1000);

		// 1. calculate pivots
		System.out.println("serverDataRecords size " + serverDataRecords.size());
		int interval = serverDataRecords.size() / numberOfProcessors;
		int localcount = 1;
		for (int i = numberOfProcessors; i < serverDataRecords.size() &&
				localcount < numberOfProcessors ; i += interval) {			
			pivArray.add(serverDataRecords.get(i).getSortValue());			
			localcount++;
		}
		
		System.out.println("My range is [" + serverDataRecords.get(0) + " to " + serverDataRecords.get(serverDataRecords.size()-1) + "]");
		System.out.println("My pivots are : " + pivArray);
		dataRecordPivotsList.addAll(pivArray);

		// 2. Send calculated pivots from this server to server-0 (master)
		if (serverNumber != 0) {
			System.out.println("sending distributePivot#" + piv + " to " + FileSystem.getServerIPaddrMap().get(0));
			outDist.get(0).writeBytes("distributePivot#start\n");
			for(Double d : dataRecordPivotsList){
				System.out.println("local pivot");
				outDist.get(0).writeBytes(d + "\n");
				System.out.println("added local pivot" + d);
				localcount++;
			}
			outDist.get(0).writeBytes("distributePivot#end\n");
			System.out.println("distributed from serverNumber: "
					+ serverNumber);
			// server number 0 only receives
		}
		
	}

	/**
	 * For server-0
	 * 	1. Set flag distributePivotON to receive local pivots from other servers
	 * 	2. Add to server-0 pivot list to make complete global pivots list
	 * 	3. Commence global pivot calculation, after local pivots from all servers received
	 * 	4. Distribute global pivots to all servers
	 * */
	private void stage3_distribute_pivots(String[] receivedResult) 
			throws IOException {
		// 1. Set flag distributePivotON to receive local pivots from other servers
		if (serverNumber == 0 && receivedResult[1].equalsIgnoreCase("start")) {
			System.out.println("Received pivots start");
			distributePivotON = true;	
		}
		
		if (serverNumber == 0 && receivedResult[1].equalsIgnoreCase("end")) {
			serversReplied++;
			System.out.println("servers Replied " + serversReplied);
		}

		if (serversReplied == totalServers - 1) {
			System.out.println("Adding to server's pivot list to make global set " + serversReplied);
			// 2. Add to server-0 pivot list to make complete global pivots list
			
			System.out.println("server data record pivot list " + serverDataRecordPivotValuesList);
			System.out.println("dataRecordPivotsList " + dataRecordPivotsList);
			
			serverDataRecordPivotValuesList.addAll(dataRecordPivotsList);
			System.out.println("Collections sort");
			Collections.sort(serverDataRecordPivotValuesList);

			System.out.println("All Sorted Pivots. Selecting global pivots");

			// 3. Commence global pivot calculation, after local pivots from all servers received
			List<Double> pivArray = new ArrayList<Double>();
			int interval = serverDataRecordPivotValuesList.size() / numberOfProcessors;
			int count = 1;
			for (int i = interval; i < serverDataRecordPivotValuesList.size() && 
					count < numberOfProcessors; i += interval) {
				pivArray.add(serverDataRecordPivotValuesList.get(i));
				count ++;
			}

			// 4. Distribute global pivots to all servers
			System.out.println("Sending global pivots " + pivArray);
			
			for (int i = 0; i < totalServers; i++) {
				outDist.get(i).writeBytes("globalpivot#start\n");

				for(int j = 0 ; j < pivArray.size() ; j++){
					outDist.get(i).writeBytes(pivArray.get(j).toString() + "\n");
				}
				
				outDist.get(i).writeBytes("globalpivot#end\n");
			}
			
			// reset serversReplied and distributePivot
			serversReplied = 0;
			distributePivotON = false;
		}	
	}
	
	/**
	 * 1. Receive global pivots from server-0
	 * 2. Create partitions of local data using global pivots
	 * 3. Send partitions to respective servers
	 * */
	private void stage4_global_pivots(String[] receivedResult) 
			throws IOException {
		if(receivedResult[1].equals("start")){
			System.out.println("STAGE 4 : "
					+ "Receive Global Pivots");	
			// 1. Receive global pivots from server-0
		}
		
		if(receivedResult[1].equals("end")){
			System.out.println("STAGE4 : "
					+ "globalpivot end" + globalDataRecordPivotValuesList);
			synchronized(this){
				serversReplied++;
					
				int count = 0, counterPivot = 0;
				
				List<List<DataRecord>> drsToBeSent = new ArrayList<>();
				System.out.println("Init partitions");
				for(int i = 0; i < globalDataRecordPivotValuesList.size() + 1; i++){
					drsToBeSent.add(i, new ArrayList<DataRecord>(1000));
				}
				System.out.println("Creating partitions for list of size " + serverDataRecords.size());
				// 2. Create partitions of local data using global pivots
				for (DataRecord drs : serverDataRecords) {
					if (counterPivot == globalDataRecordPivotValuesList.size()) {
						drsToBeSent.get(count).add(drs);
					} else if (globalDataRecordPivotValuesList.get(counterPivot) >= drs.getSortValue()){
						drsToBeSent.get(count).add(drs);
					} else {
						counterPivot++;
						count++;
						drsToBeSent.get(count).add(drs);
					}
				}
				
				
				// 3. Send partitions to respective servers
				System.out.println("Partitions created. Send to respective servers.");
	
				for (int i = 1; i < totalServers; i++) {
					
					System.out.println("Sending Partition from Server"
							+ serverNumber + " to Server" + (serverNumber + i)
							% totalServers);
					outDist.get((serverNumber + i) % totalServers).writeBytes("mypart#start\n");
					
					int sendToServerNumber = (serverNumber + i) % totalServers;
					try {
						MRFS.writeToEC2(drsToBeSent.get(sendToServerNumber), sendToServerNumber, serverNumber);
					} catch (JSchException | SftpException e) {
						e.printStackTrace();
					}
					outDist.get((serverNumber + i) % totalServers).writeBytes("mypart#end\n");
				}
				System.out.println("keeping my partition in cache");
				for(DataRecord i : drsToBeSent.get(serverNumber)){
					serverDataRecordsCache.add(i);
				}
				System.out.println("drsToBeSent set to null");
				drsToBeSent = null;
				System.out.println("serverDataRecords set to null");
				serverDataRecords = null;
				// useless now 
				serversReplied = 0;
				globalPivotON = false;
			}
		}
	}
	
	/**
	 * 1. Read this server partitions written from previous step to get complete record list
	 * 2. Sort complete record list
	 * 3. Write final output to output S3 bucket
	 * */
	private void stage5_mypart_receive_parts(String[] receivedResult, DataOutputStream outClient) throws IOException, ClassNotFoundException {
		if(receivedResult[1].equals("start")){
			System.out.println("STAGE 5: receiving my partitions");	
//			mypartON = true;
		}
		
		if(receivedResult[1].equals("end")){
		
			mypart_serversReplied++;
			
			if (mypart_serversReplied == totalServers - 1) {

				System.out.println("Begin sorting complete serverDataRecordsCache..." + serverDataRecordsCache.size());
				// 1. Read this server partitions written from previous step to get complete record list
				LocalFSSorter.writeToTempCache((ArrayList<DataRecord>) serverDataRecordsCache, MYPARTS_SORTED_COMPLETE_FILE);
				
				MRFS.mergeMyParts(serverNumber, MYPARTS_SORTED_COMPLETE_FILE);
				
				// 2. Sort complete record list
//				Collections.sort(serverDataRecordsCache);
				System.out.println("Complete partition for server sorted.");
				
				// 3. Write final output to output S3 bucket
				MRFS.writeCachePartsToOutputBucket(serverNumber, MYPARTS_SORTED_COMPLETE_FILE);
				System.out.println("Written part files to output S3 bucket.");
				outClient.writeBytes("finished"+ "\n");
			}
		}
	}
}
