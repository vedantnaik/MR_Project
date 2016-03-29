package server;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Server program which listens on the port requested or passed in the URL.
 * Contains two functions, sort and kill(itself)
 */
public class Server implements Runnable {
	private Socket connection = null;
	static ServerSocket serverSocket = null;
	static int[] ports = { 1210, 1211, 1212 };

	int p = ports.length + 1;
	static int serverNumber = 0;

	private static int serversReplied = 0;
	private static int mypart_serversReplied = 0;

	private static List<Integer> mypivsArray = new ArrayList<>();
	private static List<Integer> serverPivsArray = new ArrayList<>();
	private static List<Integer> globalPivots = new ArrayList<>();
	private static List<Integer> mypartInts = new ArrayList<>();
	
	private static boolean distributePivotON = false;
	private static boolean globalPivotON = false;
	private static boolean mypartON = false;
	private static boolean receivingMyPartitionON = false;
	static Object lock;
	// for now
	private String serverIP = "127.0.0.1";

	private static DataOutputStream outDist[] = { null, null, null };
	private static Socket[] sendingSocketDist = { null, null, null };

	private static List<Integer> myInts = new ArrayList<>();
	private static List<Integer> myInts2 = new ArrayList<>();
	

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
		if (args.length != 1) {
			System.out.println("Syntax error: Include my Number");
			System.out.println("Usage: Server <servernumber>");
			System.exit(0);
		}
		lock = new Object();
		serverNumber = Integer.parseInt(args[0]);
		int port = ports[serverNumber];
		System.out.println("Started Server ..." + serverNumber + " at " + port);
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
						stage5_mypart_receive_parts(receivedResult);

					} else if (receivedResult[0].equals("kill")) {
						System.out.println("KILLED!");
						System.exit(0);
					} else {
						if(receivingMyPartitionON){
							myInts.add(new Integer(receivedResult[0]));
						}
						else if (distributePivotON) {
							serverPivsArray.add(new Integer(receivedResult[0]));
						} else if (globalPivotON) {
							globalPivots.add(new Integer(receivedResult[0]));
						} else if (mypartON) {
							mypartInts.add(new Integer(receivedResult[0]));
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
		
		// sorting my list
		Collections.sort(myInts);

		System.out.println("Sorted " + myInts);
		
	}

	private void stage2_select_my_pivots() throws IOException {
		
		String piv = "";
		List<Integer> pivArray = new ArrayList<>();

		
		for (int i = 0; i < myInts.size() ; i += p) {
			pivArray.add(myInts.get(i));
		}
		
		System.out.println("my pivots are" + pivArray);
		mypivsArray.addAll(pivArray);

		// send only to server 0
		if (serverNumber != 0) {
			System.out.println("sending distributePivot#" + piv + "\n");
			outDist[0].writeBytes("distributePivot#start\n");
			for(int i = 0; i < myInts.size() ; i += p){
				outDist[0].writeBytes(myInts.get(i).toString() + "\n");
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

			serverPivsArray.addAll(mypivsArray);

			Collections.sort(serverPivsArray);

			System.out.println("All Sorted Pivots "	+  serverPivsArray);

			System.out.println("Selecting global pivots");

			List<Integer> pivArray = new ArrayList<>();

			for (int i = (p - 1); i < serverPivsArray.size(); i += (p - 1)) {
				pivArray.add(new Integer(serverPivsArray.get(i).intValue()));
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
					+ "globalpivot end" + globalPivots);
			synchronized(this){
			serversReplied++;

			List<Integer> copyOfglobalPivots = new ArrayList<>(
					globalPivots);

			int prevIndex = 0, index = 0;
			
			for(Integer i : myInts){
				copyOfglobalPivots.add(new Integer(i.intValue()));
			}

			
			Collections.sort(copyOfglobalPivots);
			System.out.println("copyOfglobalPivots " + copyOfglobalPivots);
			System.out.println("myInts " + myInts);
			System.out.println("globalPivots " + globalPivots);
			int count = 0;
			List<List<Integer>> integersToBeSent = new ArrayList<>();
			for (Integer i : globalPivots) {
				index = copyOfglobalPivots.lastIndexOf(i);
				 System.out.println("indexes " + prevIndex + " " + index);
				 System.out.println("sublists " + myInts.subList(prevIndex , index - count));
				integersToBeSent.add(myInts.subList(prevIndex, index
						- count));
				count++;
				prevIndex = index - count + 1;
			}
			integersToBeSent.add(myInts.subList(prevIndex,
					myInts.size()));
			
			
			System.out.println("Every Processors Partitions: "
					+ integersToBeSent);

			int serverNumb = serverNumber;

			for (int i = 1; i < 3; i++) {
				
				
				String sendig = ""+ integersToBeSent.get((serverNumb + i) % 3);
				sendig = sendig.replace("[", "").replace("]", "");
				
				System.out.println("Sending Partition from Server"
						+ serverNumb + " to Server" + (serverNumb + i)
						% 3 + " " + sendig);
				outDist[(serverNumb + i) % 3].writeBytes("mypart#start\n");
				for(int j = 0 ; 
						j < integersToBeSent.get((serverNumb + i) % 3).size(); 
						j++){
					
					outDist[(serverNumb + i) % 3].writeBytes(
							integersToBeSent.get((serverNumb + i) % 3).get(j) 
							+ "\n");
					
				}
				outDist[(serverNumb + i) % 3].writeBytes("mypart#end\n");
				
			}
			System.out.println();
			
			for(Integer i : integersToBeSent.get(serverNumber)){
				myInts2.add(new Integer(i.intValue()));
			}
			
			
			// useless now 
			serversReplied = 0;
			globalPivotON = false;
			}
//			}
		}
	}
	
	private void stage5_mypart_receive_parts(String[] receivedResult) {
		if(receivedResult[1].equals("start")){
			System.out.println("STAGE 5: receiving my partitions");	
			mypartON = true;
		}
		
		if(receivedResult[1].equals("end")){
			mypart_serversReplied++;
			for(Integer i : mypartInts){
				myInts2.add(new Integer(i.intValue()));
			}
			System.out.println("mypart_serversReplied " 
			+ mypart_serversReplied + " mypartInts " + mypartInts);
			mypartInts = new ArrayList<>();

			if (mypart_serversReplied == ports.length - 1) {
				Collections.sort(myInts2);
				System.out.println("Global Sorted Partition: " + myInts2);
			}
		}
		
	}
}
