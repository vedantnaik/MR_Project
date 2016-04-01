package client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import utils.FileSystem;

/**
 * A sample client which connects to the server and can issue a sorting command
 * with the contents of a string. It can also kill the server and self destruct
 * itself!
 *
 */
public class Client {

	static int port = 1210;
	
	private static Map<Integer, DataOutputStream> out = null;
	private static Map<Integer, Socket> sendingSocket = null;
	private static Map<Integer, BufferedReader> inFromServer = null;
	private static int totalServers;
	
	static FileSystem MRFS;
	private static Map<Integer, String> servers;
	
	/**
	 * The Client constructor when called, connects to all 
	 * the Server instances and cannot start before the Servers do.
	 * Maintains an Map of Sockets to talk to all the servers and 
	 * do the handshake process.
	 */
	public Client() {
		try {
			servers = FileSystem.getServerIPaddrMap();
			totalServers = servers.size();
			out = new HashMap<>(2 * totalServers);
			sendingSocket = new HashMap<>(2 * totalServers);
			inFromServer = new HashMap<>(2 * totalServers);
			System.out.println("servers are " + servers);
			for (int i = 0; i < totalServers; i++) {

				sendingSocket.put(i, new Socket(servers.get(i), port));
				Socket socketTmp = sendingSocket.get(i);
				socketTmp.setSoTimeout(0);
				sendingSocket.put(i, socketTmp);
				out.put(i, new DataOutputStream(sendingSocket
						.get(i).getOutputStream()));
				inFromServer.put(i ,new BufferedReader(new InputStreamReader(
				sendingSocket.get(i).getInputStream())));

			}
		} catch (IOException e) {
			System.out.println("Unable to connect to all servers!");
			e.printStackTrace();
		}
	}

	/**
	 * 1. Calls the server with sort function and sends the file inputs as String
	 * 	  after reading.
	 * 2. It then waits for the Server to reply back when their local sorting
	 *    is done. 
	 * 3. Then client issues a command to all Servers to distribute their 
	 * 	  pivots.
	 * 4. The client then waits for the servers to reply when they finish
	 *    and issues the self-destruct command to kill servers.
	 * @param fileName the filename to be read. If not found returns to 
	 * 			main loop and continues to ask
	 * @param serverIP the server ip of the server eg. 127.0.0.1
	 * @param serverPort the server port eg. 1212
	 * @throws IOException 
	 */
	public void callSorter(String inputBucketName, String outputBucketName) throws IOException {
		
		FileSystem myS3FS = new FileSystem(inputBucketName, outputBucketName);
		
		HashMap<Integer, ArrayList<String>> partsMap = myS3FS.getS3Parts(totalServers); 

		try {
			
			 // 1. Calls the server with sort function and sends the file inputs as String
			 //	  after reading.
			for (int i = 0; i < totalServers; i++) {
				
				out.get(i).writeBytes("sort#start\n");
				System.out.println("Map Parts to " + servers.get(i) + " => " + partsMap.get(i));
				for(String fileNameIter : partsMap.get(i)){
					out.get(i).writeBytes(fileNameIter+"\n");
				}
				
				out.get(i).writeBytes("sort#end\n");
				System.out.println("Running job on Server ..." + i);
			}

			// 2. It then waits for the Server to reply back when their local sorting
		    //   is done. 
			int replies = 0;
			boolean[] replied = new boolean[totalServers];
			while (true) {
				for (int i = 0; i < replied.length && !replied[i]; i++) {

					String result = inFromServer.get(i).readLine();
					String[] returnedResult = result.split("#");
					System.out.println(returnedResult[0] + " Results from : " + i);

					replied[i] = true;
					if (replied[i]) {
						replies++;
					}
				}
				if (replies == totalServers) {
					System.out.println("replied by " + replies);
					break;
				}

			}
			// totalServers replied
			System.out.println("replied array " + Arrays.toString(replied));

			
			// 3. Then client issues a command to all Servers to distribute their 
			//  	  pivots.
			System.out.println("sending command to distribute now!");
			// distribute pivots
			for (int i = 0; i < totalServers; i++) {
				out.get(i).writeBytes("distribute#All" + "\n");
			}
			System.out.println("distributed!");
			
			
			
			System.out.println("Waiting for Servers to finish");
			//4. The client then waits for the servers to reply when they finish
			//    and issues the self-destruct command to kill servers.
			
			// last reply phase phase
			replies = 0;
			Arrays.fill(replied, false);
			while (true) {
				for (int i = 0; i < replied.length && !replied[i]; i++) {

					String result = inFromServer.get(i).readLine();
					String[] returnedResult = result.split("#");
					System.out.println(returnedResult[0] + " Results from : " + i);

					replied[i] = true;
					if (replied[i]) {
						replies++;
					}
				}
				if (replies == totalServers) {
					System.out.println("Servers Finished " + replies);
					System.out.println("Killing all servers!");
					killer();
					System.out.println("Killed Server, Self Destruct!");
					System.exit(0);
				}

			}

		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
			e.printStackTrace();
		}
	}

	/**
	 * Sends a kill command to server to terminate itself
	 * 
	 * @param serverIP the server ip of the server eg. 127.0.0.1
	 * @param serverPort the server port eg. 1212
	 */
	private void killer() {
		try {
			for (int i = 0; i < totalServers; i++) {
				Socket sendingSocket = new Socket(servers.get(i), port);
				DataOutputStream out = new DataOutputStream(
						sendingSocket.getOutputStream());
				out.writeBytes("kill#" + "\n");
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
	public static void main(String args[]) throws Exception {
		if (args.length != 2) {
			System.out
					.println("Include Server IP Address. Currently only localhost!");
			System.out.println("Usage Client <inputBucketname> <outputBucketName>");
			System.exit(0);
		}

		String inputBucketName = args[0];
		String outputBucketName = args[1];
		
		System.out.println("Reading s3 bucket");
		MRFS = new FileSystem(inputBucketName, outputBucketName);
		System.out.println("connecting to servers");
		Client client = new Client();
		System.out.println("Connected to all Servers!");
		System.out.println("Informing servers to begin!");
		client.callSorter(inputBucketName, outputBucketName);

	}
}
