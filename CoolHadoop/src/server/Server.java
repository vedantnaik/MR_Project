
package server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import utils.Constants;
import coolmapreduce.Configuration;
import coolmapreduce.Job;
import coolmapreduce.MapperHandler;
import coolmapreduce.ReducerHandler;
import fs.FileSys;
import fs.shuffler.LoadDistributor;

/**
 * Server program which listens on the port 1210 or local-port 
 * as defined by the program. It has the capability to listen and
 * execute commands as per Master. 
 * @author Dixit_Patel
 */
public class Server implements Runnable {
	private Socket connection = null;
	static ServerSocket serverSocket = null;

	static int port = 1210;
	static int numberOfProcessors = 0;
	static int serverNumber = 0;

	// the lock Object for barriers
	static Object lock;

	// List of Sockets and OutputStream for communication
	private static Map<Integer, DataOutputStream> outDist = null;
	private static Map<Integer, Socket> sendingSocketDist = null;
	private static DataOutputStream outClient = null;
	private static int totalServers;

	// The MapperHandler and reduceHandler classes which call the 
	// map and reduce functions of the framework
	private static MapperHandler mapperhandlerInstance = null;
	private static ReducerHandler reducerhandlerInstance = null;

	private static Configuration config;
	private static Job job;
	
	// booleans
	private static boolean localServersFlag = false; 
	private static boolean receivingMapFiles = false;

	private static Map<String, Object> masterKeyServerMap;
	
	/**
	 * The Server which accepts a connection and starts a new
	 * Runnable for each connection
	 * @param newConnection the Socket object which starts the connection
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public Server(Socket newConnection) throws UnknownHostException,
			IOException {
		this.connection = newConnection;
		initOtherSockets(localServersFlag);
	}

	/**
	 * Init the DataOutputStream, Socket and BufferedReader in a HashMap
	 * according to the server-number for easy access for upcoming
	 * commmunication between other Slave Servers
	 * 
	 * @param localServersFlag
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void initOtherSockets(boolean localServersFlag) throws UnknownHostException, IOException {
		System.out.println("Init Sockets");
		
		// hacking localport 
		int localport = port - serverNumber;
			
		for (int i = 0; i < totalServers; i++) {
			if (outDist.get(i) == null) {
				if(localServersFlag){
					System.out.println("Connecting to " + i + " @ " + (localport + i));
					sendingSocketDist.put(i, new Socket("localhost", (localport + i)));
				}else{
					sendingSocketDist.put(i, new Socket(config.getServerIPaddrMap().get(i), port));
				}	
				outDist.put(i, new DataOutputStream(
						sendingSocketDist.get(i).getOutputStream()));
			}
		}
	}

	public static void main(String args[]) throws Exception {
		if (args.length < 1) {
			System.out.println("Syntax error: Include my Number");
			System.out
					.println("Usage: Server <servernumber> <LOCAL/nothing>");
			System.out.println("Server 0 LOCAL");
			System.out.println("or");
			System.out.println("Server 0");
			System.exit(0);
		}
		serverNumber = Integer.parseInt(args[0]);

		if(args.length > 1 && args[1].equals(Constants.LOCAL))
			localServersFlag = true;

		System.out.println("Running Local? : " + localServersFlag);

		config = new Configuration();
		
		lock = new Object();

		totalServers = config.getServerIPaddrMap().size();
		numberOfProcessors = totalServers;
		System.out.println("totalServers "
				+ config.getServerIPaddrMap().size());
		
		outDist = new HashMap<>(2 * totalServers);
		sendingSocketDist = new HashMap<>(2 * totalServers);

		System.out.println("servers to connect to : "
				+ config.getServerIPaddrMap());

		if (localServersFlag) {
			port = port + serverNumber;
		}

		try {
			serverSocket = new ServerSocket(port);
			System.out.println("Started Server " + serverNumber + " => " + port);

		} catch (IOException e) {
			System.out.println("Death on port " + port + 
					" Try some other port");
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
	 * Starts a thread to server the request given by the
	 * Master program. It can 
	 * * <p><ul>
	 * <li> Read the serialized job from the jobfile. 
	 *      Reply JOBREAD to the Master 
	 * <li> 1. Receive the file names to the slaves about which 
	 *      files to read and communicate a end of transmission: FILES_READ
	 * <li> 3. Receive an instruction to start reading the files. 
	 * <li> 4. Send a MAP_FINISH instruction to Master. 
	 * <li> 5. Receive a SHUFFLEANDSORT instruction from Master
	 * <li> 6. Send an ACK for SHUFFLEFINISH 
	 * <li> 7. Wait for instruction to start REDUCE phase 
	 * <li> 8. Reply for REDUCEFINISH from all servers.
	 * </ul><p>
	 * 
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
					
					String[] receivedResult = splitReceived(received);

					if (receivedResult[0].equals(Constants.READJOB)) {
						System.out.println("STAGE 0 " + Constants.READJOB);											
						read_serialized_job(receivedResult, out);
					} 					
					else if (receivedResult[0].equals(Constants.MAPFILES)) {
						System.out.println("STAGE 1 " + Constants.MAPFILES);											
						start_map_files_phase(receivedResult, out);
					} 
					else if (receivedResult[0].equals(Constants.MAP)) {
						System.out.println("STAGE 2 " + Constants.MAP);
						start_map_phase();
					}else if(receivedResult[0].equals(Constants.SHUFFLEANDSORT)){
						System.out.println("STAGE 3 " + Constants.SHUFFLEANDSORT);
						start_shuffle_and_sort();
					}else if(receivedResult[0].equals(Constants.REDUCE)){
						System.out.println("STAGE 4 " + Constants.REDUCE);
						start_reduce_phase();
					}
					else if (receivedResult[0].equals(Constants.KILL)) {
						System.out.println("STAGE N " + Constants.KILL);
						System.out.println("KILLED!");
						System.exit(0);
					} else {
						if(receivingMapFiles){							
							// add received filename to list
							System.out.println("file received " + receivedResult[0]);
							mapperhandlerInstance.addToListOfMapperFiles(receivedResult[0]);
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

	/**
	 * Read the Serialized jobfile name as send by the Master program.
	 * Reply back to the Master saying JOBREAD
	 * @param receivedResult the Request send by Master Program
	 * @param out the DataOutputStream to reply back to client on
	 */
	private void read_serialized_job(String[] receivedResult,
			DataOutputStream out) {

		try {
			System.out.println("STAGE 0 " + Constants.READJOB);
			System.out.println("Reading Jobname: " + receivedResult[1]);

			ObjectInputStream iis = new ObjectInputStream(new FileInputStream(
					new File(receivedResult[1]+Constants.JOBEXTN)));
			job = (Job) iis.readObject();
			iis.close();
			System.out.println("job filesize " + new File(receivedResult[1]+Constants.JOBEXTN).length());
			System.out.println("job details " + job);
			outClient = out;
			outClient.writeBytes(Constants.JOBREAD + "#" + serverNumber + "\n");

		} catch (IOException | ClassNotFoundException e) {
			System.out.println("Error in Sending Data via Sockets or");
			System.out.println("Job class not found");
			e.printStackTrace();
		}

	}

	/**
	 * A splitter for the Protocol sent by the Master. 
	 * The Protocol consists of instructions seperated by "#"
	 * @param received the received protocol
	 * @return the array of protocols split by "#"
	 */
	private String[] splitReceived(String received){
		String[] receivedResult = new String[2];
		if (received.contains("#")) {
			receivedResult = received.split("#");
		} else {
			receivedResult[0] = received;
		}
		return receivedResult;
	}
	
	/**
	 * Start the Map Files Phase. Collect all the filenames as 
	 * send by the Master program. After the END protocol
	 * send an ACK back to the Master saying done reading files.	 * 
	 * @param receivedResult The array of protocols received
	 * @param out the DataOutputStream to reply back to client on
	 */
	private void start_map_files_phase(String[] receivedResult,
			DataOutputStream out) {

		try {
			if (receivedResult[1].contains(Constants.START)) {
				System.out.println("STAGE 1 start");
				mapperhandlerInstance = new MapperHandler(job, serverNumber);
				receivingMapFiles = true;
			}

			if (receivedResult[1].contains(Constants.END)) {
				System.out.println("STAGE 1 " + "receiving files ends");

				receivingMapFiles = false;
				
				outClient.writeBytes(
						Constants.FILES_READ + "#" + serverNumber + "\n");
			}
		} catch (IOException e) {
			System.out.println("Error in Sending Data via Sockets");
			e.printStackTrace();
		}

	}


	/**
	 * Call the MapperHandler to runMapperHandler which calls
	 * map function as defined by the defining class using
	 * reflection 
	 * @throws IOException 
	 */
	private void start_map_phase() throws IOException {
		System.out.println("Stage1: read input files assigned to server...");

		// Calling Mapper Handler thread
		System.out.println("Mapper Starts");

		try {
			mapperhandlerInstance.runMapperHandler();
		} catch (Exception e) {
			e.printStackTrace();
			// send to Master only
			System.out.println("Sending " + 
					Constants.MAPFAILURE + " to Master");
			outDist.get(0).writeBytes(
					Constants.MAPFAILURE + "#" + serverNumber + "\n");
		}
		System.out.println("Mapper Ends");

		// reply MAPFINISH#Number to Master
		outClient.writeBytes(Constants.MAP_FINISH + "#" + serverNumber + "\n");
	}
	

	/**
	 * Call the shuffle and sort phase
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	private void start_shuffle_and_sort() throws IOException {
		// read the master MKM and send the files to the reducer 
		// for corresponding server#
		masterKeyServerMap = null;
		try {
		String masterBroadcastKeyServerMap = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER
				.replace("<JOBNAME>", job.getJobName()) + Constants.BROADCAST_KEY_SERVER_MAP;
		
		ObjectInputStream iis = new ObjectInputStream(new FileInputStream(
				new File(masterBroadcastKeyServerMap)));
		masterKeyServerMap = (HashMap<String, Object>) iis.readObject();
		iis.close();
		
		} catch (ClassNotFoundException e) {
			System.out.println("Error in reading the master broadcast map");
			e.printStackTrace();
		}
		System.out.println("moving Values Files To Reducer Input Locations");
		LoadDistributor.makeAllKeyFolderLocations(masterKeyServerMap, job.getJobName());

		LoadDistributor.moveValuesFilesToReducerInputLocations(masterKeyServerMap, serverNumber, job);
		
		outClient.writeBytes(
				Constants.SHUFFLEFINISH + "#" + serverNumber + "\n");
	}
	
	/**
	 * Call the reduce phase
	 * @throws IOException
	 */
	private void start_reduce_phase() throws IOException {
		
		HashMap<String, Object> masterMKMs = null;
		try {
			// read mkm from file
			String masterBroadcastMKM = Constants.MASTER_MAPPER_KEY_MAPS_FOLDER
					.replace("<JOBNAME>", job.getJobName()) + Constants.BROADCAST_MKM_MAP;
			
			ObjectInputStream iis = new ObjectInputStream(new FileInputStream(
					new File(masterBroadcastMKM)));
			masterMKMs = (HashMap<String, Object>) iis.readObject();
			iis.close();
		} catch (ClassNotFoundException e1) {
			System.err.println("Could not cast MKM to an object of hashmap");
			e1.printStackTrace();
		}
		
		// 1. merge the values from different servers for each key, to values.txt
		FileSys.mergeValuesForAllKeysForJob(job, serverNumber, masterMKMs, masterKeyServerMap);
		
		// 2. create reducer handler
		//  - b. for each key, read values.txt using the iterator and make reduce call.
		try {
			reducerhandlerInstance = new ReducerHandler(job, masterMKMs, serverNumber);
			reducerhandlerInstance.runReducerHandler(masterKeyServerMap);
		} catch (Exception e) {
			System.err.println("Error in run reducer handler from server "+serverNumber);
			e.printStackTrace();
		}
		
		outClient.writeBytes(
				Constants.REDUCEFINISH + "#" + serverNumber + "\n");
	}
}
