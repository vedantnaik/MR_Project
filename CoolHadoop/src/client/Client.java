package client;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Map;

import coolmapreduce.Configuration;
import utils.Constants;


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
	private String currentServer = null;
	private static Configuration config;
	private static Map<Integer, String> servers;
	

	public Client() {
		try {
			servers = Configuration.getServerIPaddrMap();
			totalServers = servers.size();
			out = new HashMap<>(2 * totalServers);
			sendingSocket = new HashMap<>(2 * totalServers);
			inFromServer = new HashMap<>(2 * totalServers);
			System.out.println("servers are " + servers);
			currentServer = null;
			for (int i = 0; i < totalServers; i++) {
				currentServer = servers.get(i);
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
			System.out.println("Unable to connect to all servers! " + currentServer);
			
			e.printStackTrace();
			System.exit(0);
		}
	}


	public void callSorter(Configuration config, String inputBucketName, String outputBucketName, 
			String inputFolder, String outputFolder) throws IOException {
		
		
//		FileSystem myS3FS = new FileSystem(inputBucketName, outputBucketName, inputFolder, outputFolder);
		
		// TODO: Un-null me 
		HashMap<Integer, ArrayList<String>> partsMap = null;//myS3FS.getS3Parts(totalServers); 

		try {
			
			 // 1. Sending files to server
			for (int i = 0; i < totalServers; i++) {
				
				out.get(i).writeBytes(Constants.MAPFILES+ "#" + Constants.START+ "\n");
				System.out.println("Map Parts to " + servers.get(i) + " => " + partsMap.get(i));
				for(String fileNameIter : partsMap.get(i)){
					out.get(i).writeBytes(fileNameIter+"\n");
				}
				
				out.get(i).writeBytes(Constants.MAPFILES+ "#" + Constants.END+ "\n");
				System.out.println("Files sent to Server ..." + i);
			}
			
			
			// 3. Then client issues a command to all Servers to distribute their 
			//  	  pivots.
			System.out.println("sending command to start Map");
			// distribute pivots
			for (int i = 0; i < totalServers; i++) {
				out.get(i).writeBytes(Constants.MAP+ "#" + Constants.START + "\n");
			}
			System.out.println("distributed!");

			
			// WAITING LOGIC, after reduce!!
			
			// 2. It then waits for the Server to reply back when their local sorting
		    //   is done. 
//			int replies = 0;
//			boolean[] replied = new boolean[totalServers];
//			while (true) {
//				for (int i = 0; i < replied.length && !replied[i]; i++) {
//
//					String result = inFromServer.get(i).readLine();
//					String[] returnedResult = result.split("#");
//					System.out.println(returnedResult[0] + " Results from : " + i);
//
//					replied[i] = true;
//					if (replied[i]) {
//						replies++;
//					}
//				}
//				if (replies == totalServers) {
//					System.out.println("replied by " + replies);
//					break;
//				}
//
//			}
//			// totalServers replied
//			System.out.println("replied array " + Arrays.toString(replied));
			
			
			System.out.println("Waiting for Servers to finish");

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
		if (args.length != 4) {
			System.out
					.println("Include Server IP Address. Currently only localhost!");
			System.out.println("Usage Client <inputBucketname> <outputBucketName>");
			System.exit(0);
		}

		String inputBucketName = args[0];
		String outputBucketName = args[1];
		String inputFolder = args[2];
		String outputFolder = args[3];
		
		System.out.println("Input bucket: " + inputBucketName);
		System.out.println("Output bucket: " + outputBucketName);
		System.out.println("Input folder: " + inputFolder);
		System.out.println("Output folder: " + outputFolder);
		
		System.out.println("Reading s3 bucket");
//		MRFS = new FileSystem(inputBucketName, outputBucketName, inputFolder, outputFolder);
		config = new Configuration();
		System.out.println("connecting to servers");
		Client client = new Client();
		System.out.println("Connected to all Servers!");
		System.out.println("Informing servers to begin!");
		client.callSorter(config, inputBucketName, outputBucketName, inputFolder, outputFolder);

	}
}
