package server;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import datafile.DataRecord;
import utils.FileSystem;
import utils.S3FileReader;

/**
 * Server program which listens on the port requested or passed in the URL.
 * Contains two functions, sort and kill(itself)
 */
public class Server implements Runnable {
	private Socket connection = null;
	static ServerSocket serverSocket = null;
	static int[] ports = { 1210, 1211, 1212 };

	int numberOfProcessors = ports.length + 1;
	static int serverNumber = 0;

	private static int serversReplied = 0;
	private static int mypart_serversReplied = 0;

	private static List<Double> dataRecordPivotsList = new ArrayList<Double>();
	private static List<Double> serverDataRecordPivotValuesList = new ArrayList<Double>();
	private static List<Double> globalDataRecordPivotValuesList = new ArrayList<Double>();
	private static List<Double> stage5ReadDataRecordList = new ArrayList<Double>();
	
//	private static List<Integer> mypivsArray = new ArrayList<>();
//	private static List<Integer> serverPivsArray = new ArrayList<>();
//	private static List<Integer> globalPivots = new ArrayList<>();
//	private static List<Integer> mypartInts = new ArrayList<>();
	
	private static boolean distributePivotON = false;
	private static boolean globalPivotON = false;
	private static boolean mypartON = false;
	private static boolean receivingMyPartitionON = false;
	static Object lock;
	// for now
	private String serverIP = "127.0.0.1";

	private static DataOutputStream outDist[] = { null, null, null };
	private static Socket[] sendingSocketDist = { null, null, null };

//	private static List<Integer> myInts = new ArrayList<>();
//	private static List<Integer> myInts2 = new ArrayList<>();
	
	
	
	///// DataRecord sort from s3
	
	private static String bucketName;
	private static List<String> fileNameList = new ArrayList<String>();
	
	private static List<DataRecord> serverDataRecords = new ArrayList<DataRecord>();
	private static List<DataRecord> serverDataRecordsCache = new ArrayList<DataRecord>();
	
	///// File system
	
	static FileSystem MRFS;
	
	/////
	

	public Server(Socket newConnection) throws UnknownHostException,
			IOException {
		this.connection = newConnection;

		initOtherSockets();
	}

	public void initOtherSockets() throws UnknownHostException, IOException {
		for (int i = 0; i < ports.length; i++) {
			if (outDist[i] == null) {
				sendingSocketDist[i] = new Socket(serverIP, ports[i]);
				outDist[i] = new DataOutputStream(
						sendingSocketDist[i].getOutputStream());
			}
		}
	}

	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out.println("Syntax error: Include my Number");
			System.out.println("Usage: Server <servernumber> <BucketName>");
			System.exit(0);
		}
		
		
		bucketName = args[1];
		
		MRFS = new FileSystem(bucketName);
		
		lock = new Object();
		serverNumber = Integer.parseInt(args[0]);
		int port = ports[serverNumber];
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
					DataOutputStream out = new DataOutputStream(
							connection.getOutputStream());
					System.out.println("received " + received);
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
						// TODO
						/////////////////////////////////////////////////////
						stage5_mypart_receive_parts(receivedResult);
						/////////////////////////////////////////////////////
						
					} else if (receivedResult[0].equals("kill")) {
						System.out.println("KILLED!");
						System.exit(0);
					} else {
						if(receivingMyPartitionON){
							
							// add received filename to list
							fileNameList.add(receivedResult[0]);
							
							
//							myInts.add(new Integer(receivedResult[0]));
						}
						else if (distributePivotON) {
							serverDataRecordPivotValuesList.add(Double.parseDouble((receivedResult[0])));
						} else if (globalPivotON) {
							globalDataRecordPivotValuesList.add(Double.parseDouble(receivedResult[0]));
						} else if (mypartON) {
							stage5ReadDataRecordList.add(Double.parseDouble(receivedResult[0]));
						}
					}
					lock.notifyAll();
				}
			}
		} catch (Exception e) {
			System.out.println("Thread cannot serve connection");
			e.printStackTrace();
		}
	}

	
	private void stage1_sort_my_partition() {
		
		// TODO: later make this in threads

		for (String fileName : fileNameList){
			try {
				S3FileReader s3fr = new S3FileReader(bucketName, fileName);
				serverDataRecords.addAll(MRFS.readRecordsFrom(bucketName, fileName));
				
			} catch (IOException e) {
				System.err.println("SERVER : Stage 1 Sorting : unable to read file");
				e.printStackTrace();
			}
		}
		Collections.sort(serverDataRecords);
	}

	private void stage2_select_my_pivots() throws IOException {
		
		String piv = "";
		List<Double> pivArray = new ArrayList<Double>();

		
		for (int i = 0; i < serverDataRecords.size() ; i += numberOfProcessors) {
			pivArray.add(serverDataRecords.get(i).getSortValue());
		}
		
		System.out.println("my pivots are : " + pivArray);
		dataRecordPivotsList.addAll(pivArray);

		// send only to server 0
		if (serverNumber != 0) {
			System.out.println("sending distributePivot#" + piv + "\n");
			outDist[0].writeBytes("distributePivot#start\n");
			for(int i = 0; i < serverDataRecords.size() ; i += numberOfProcessors){
				outDist[0].writeBytes(serverDataRecords.get(i).getSortValue() + "\n");
			}
			outDist[0].writeBytes("distributePivot#end\n");
			System.out.println("distributed from serverNumber: "
					+ serverNumber);
			// server number 0 only receives
		}
		
	}

	
	private void stage3_distribute_pivots(String[] receivedResult) 
			throws IOException {
		if (serverNumber == 0 && receivedResult[1].equalsIgnoreCase("start")) {

			System.out.println("Received pivots start");
			distributePivotON = true;	
			
		}
		
		if (serverNumber == 0 && receivedResult[1].equalsIgnoreCase("end")) {
			serversReplied++;
		}

		if (serversReplied == ports.length - 1) {
			// adding my own pivs

			serverDataRecordPivotValuesList.addAll(dataRecordPivotsList);

			Collections.sort(serverDataRecordPivotValuesList);

			System.out.println("All Sorted Pivots "	+  serverDataRecordPivotValuesList);

			System.out.println("Selecting global pivots");

			List<Double> pivArray = new ArrayList<Double>();

			for (int i = (numberOfProcessors - 1); i < serverDataRecordPivotValuesList.size(); i += (numberOfProcessors - 1)) {
				pivArray.add(serverDataRecordPivotValuesList.get(i));
			}

			System.out.println("sending global pivots " + pivArray);
			System.out.println("NEXT STAGE UNCLEAR!!");
			
			for (int i = 0; i < ports.length; i++) {
				outDist[i].writeBytes("globalpivot#start\n");

				for(int j = 0 ; j < pivArray.size() ; j++){
					outDist[i].writeBytes(pivArray.get(j).toString() + "\n");
				}
				
				outDist[i].writeBytes("globalpivot#end\n");
			}
			
			// serversReplied is useless now
			// so is distributePivot flag
			serversReplied = 0;
			distributePivotON = false;
		}
		
	}
	private void stage4_global_pivots(String[] receivedResult) 
			throws IOException {
		if(receivedResult[1].equals("start")){
			System.out.println("STAGE 4 : "
					+ "Receive Global Pivots");	
			globalPivotON = true;
		}
		
		if(receivedResult[1].equals("end")){
			System.out.println("STAGE4 : "
					+ "globalpivot end" + globalDataRecordPivotValuesList);
			synchronized(this){
			serversReplied++;
				
			int count = 0, counterPivot = 0;
			List<List<Integer>> integersToBeSent = new ArrayList<>();

			
			List<List<DataRecord>> drsToBeSent = new ArrayList<>();
			
			
			for(int i = 0; i < globalDataRecordPivotValuesList.size() + 1; i++){
				drsToBeSent.add(i, new ArrayList<DataRecord>());
			}
			
			for (DataRecord drs : serverDataRecords) {

				if (counterPivot == globalDataRecordPivotValuesList.size()) {
					drsToBeSent.get(count).add(new DataRecord(drs));
				} else if (globalDataRecordPivotValuesList.get(counterPivot) > drs.getSortValue()
						|| globalDataRecordPivotValuesList.get(counterPivot) == drs.getSortValue()) {
					drsToBeSent.get(count).add(new DataRecord(drs));
				} else {
					counterPivot++;
					count++;
					drsToBeSent.get(count).add(new DataRecord(drs));

				}
				
			}
			
			
	
			System.out.println("Every Processors Partitions=> "
					+ drsToBeSent);

//			int serverNumb = serverNumber;

			for (int i = 1; i < 3; i++) {
				// Write to ec2
				
				
				
				
				String sendig = ""+ drsToBeSent.get((serverNumber + i) % 3);
				sendig = sendig.replace("[", "").replace("]", "");
				
				System.out.println("Sending Partition from Server"
						+ serverNumber + " to Server" + (serverNumber + i)
						% 3 + " " + sendig);
				outDist[(serverNumber + i) % 3].writeBytes("mypart#start\n");
				
				int sendToServerNumber = (serverNumber + i) % 3;
				try {
					MRFS.writeToEC2(drsToBeSent.get(sendToServerNumber), sendToServerNumber, serverNumber);
				} catch (JSchException | SftpException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
								
				outDist[(serverNumber + i) % 3].writeBytes("mypart#end\n");
			}
			
			
			
			System.out.println();
			
			for(DataRecord i : drsToBeSent.get(serverNumber)){
				serverDataRecordsCache.add(new DataRecord(i));
			}
			
			
			// useless now 
			serversReplied = 0;
			globalPivotON = false;
			}

		}
	}
	
	private void stage5_mypart_receive_parts(String[] receivedResult) throws IOException, ClassNotFoundException {
		if(receivedResult[1].equals("start")){
			System.out.println("STAGE 5: receiving my partitions");	
			mypartON = true;
		}
		
		
		if(receivedResult[1].equals("end")){
		
			mypart_serversReplied++;
			
			
			// read server specific parts in list
			// sort 
			
			// filename
			
			
			
			if (mypart_serversReplied == ports.length - 1) {

// TODO:
				
				
				// read my parts file
				
				serverDataRecordsCache.addAll(MRFS.readMyParts(serverNumber));
				
				
//				for (int ser = 0; ser < ports.length; ser++){
//					File folderIn = new File(ser+"/");
//					
//					for(File partFile : folderIn.listFiles()){
//						if(partFile.getName().contains("p"+serverNumber)){
//							System.out.println("Server "+serverNumber + " reading at " + partFile.getName());
//							FileInputStream fileStream = new FileInputStream(folderIn+"/"+partFile.getName());
//							ObjectInputStream ois = new ObjectInputStream(fileStream);
//							
//							ArrayList<DataRecord> readList = (ArrayList<DataRecord>) ois.readObject();
//							System.out.println("read list " + readList + "");
//							// TODO: MAKE MERGER
//							serverDataRecordsCache.addAll(readList);
//						}
//					}
//				}
				
				
			
				Collections.sort(serverDataRecordsCache);
				System.out.println("Global Sorted Partition: " + serverDataRecordsCache);
				for (DataRecord dr : serverDataRecordsCache){
					System.out.println("> " + dr.getSortValue());
				}
			}
			
			
		}
		
	}
}
