package client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import utils.FileSystem;

/**
 * A sample client which connects to the server and can issue a sorting command
 * with the contents of a string. It can also kill the server and self destruct
 * itself!
 *
 */
public class Client {
	
//	static int[] ports = { 1210, 1211, 1212 };
	static int port = 1210;
//	Socket[] sendingSocket = { null, null, null };
//	DataOutputStream[] out = { null, null, null };
//	BufferedReader[] inFromServer = { null, null, null };
	
	private static Map<Integer, DataOutputStream> out = null;
	private static Map<Integer, Socket> sendingSocket = null;
	private static Map<Integer, BufferedReader> inFromServer = null;
	private static int totalServers;
	
	static FileSystem MRFS;
	private static Map<Integer, String> servers;
	
	public Client() {
		try {
			servers = FileSystem.getServerIPaddrMap();
			totalServers = servers.size();
			out = new HashMap<>(2 * totalServers);
			sendingSocket = new HashMap<>(2 * totalServers);
			inFromServer = new HashMap<>(2 * totalServers);
			System.out.println("servers are " + servers);
			for (int i = 0; i < totalServers; i++) {
//				if (out.get(i) == null) {
					sendingSocket.put(i, new Socket(servers.get(i), port));
					out.put(i, new DataOutputStream(sendingSocket
							.get(i).getOutputStream()));
					inFromServer.put(i ,new BufferedReader(new InputStreamReader(
					sendingSocket.get(i).getInputStream())));
//				}
			}
		} catch (IOException e) {
			System.out.println("Unable to connect to all servers!");
			e.printStackTrace();
		}
	}

	/**
	 * Calls the server with sort function and sends the file inputs as String
	 * after reading
	 * 
	 * @param fileName
	 *            the filename to be read. If not found returns to main loop and
	 *            continues to ask
	 * @param serverIP
	 *            the server ip of the server eg. 127.0.0.1
	 * @param serverPort
	 *            the server port eg. 1212
	 * @throws IOException 
	 */
	public void callSorter(String fileName) throws IOException {
		
		FileSystem myS3FS = new FileSystem("cs6240sp16");
		
		HashMap<Integer, ArrayList<String>> partsMap = myS3FS.getS3Parts(totalServers); 
		System.out.println("Map Parts " + partsMap);
		try {

			for (int i = 0; i < totalServers; i++) {

//				sendingSocket.put(i, new Socket(servers.get(i), port));
//				out.put(i, new DataOutputStream(
//						sendingSocket.get(i).getOutputStream()));
//				inFromServer.put(i ,new BufferedReader(new InputStreamReader(
//						sendingSocket.get(i).getInputStream())));
				
				out.get(i).writeBytes("sort#start\n");
				System.out.println("Map Parts to " + servers.get(i) + " => " + partsMap.get(i));
				for(String fileNameIter : partsMap.get(i)){
					out.get(i).writeBytes(fileNameIter+"\n");
				}
				
				out.get(i).writeBytes("sort#end\n");
				System.out.println("Running job on Server ..." + i);

			}

			// done phase
			int replies = 0;
			boolean[] replied = new boolean[totalServers];
			while (true) {
				for (int i = 0; i < replied.length && !replied[i]; i++) {

					String result = inFromServer.get(i).readLine();
					String[] returnedResult = result.split("#");
					System.out.println(returnedResult[0] + " Results from : "
							+ i);

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
			// 3 people replied
			System.out.println("replied array " + Arrays.toString(replied));

			System.out.println("sending command to distribute now!");
			// distribute pivots
			for (int i = 0; i < totalServers; i++) {
				out.get(i).writeBytes("distribute#All" + "\n");
			}
			System.out.println("distributed!");

		} catch (Exception e) {
			System.out.println("Cannot connect to the Server");
			e.printStackTrace();
		}
	}

	/**
	 * Sends a kill command to server to terminate itself
	 * 
	 * @param serverIP
	 *            the server ip of the server eg. 127.0.0.1
	 * @param serverPort
	 *            the server port eg. 1212
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
	 * main driver program
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out
					.println("Include Server IP Address. Currently only localhost!");
			System.out.println("Usage Client <bucketname>");
			System.exit(0);
		}
		BufferedReader din = new BufferedReader(
				new InputStreamReader(System.in));

		String bucketName = args[0];
		System.out.println("reading s3 bucket");
		MRFS = new FileSystem(bucketName);
		System.out.println("connecting to servers");
		Client client = new Client();
		System.out.println("Connected to all Servers!");
		while (true) {
			System.out.println("1 - Enter File Name to Sort");
			System.out.println("9 - Self Destruct");
			System.out.print("Enter : ");
			String line = din.readLine();

			if (line.equals("1")) {
				System.out.print("Enter filename (from current directory) : ");
				String fileName = "C:\\Users\\Dixit_Patel\\Google Drive\\"
						+ "Working on a dream\\StartStudying\\sem4\\MapReduce\\"
						+ "homeworks\\hw8-Distributed Sorting\\MR_Project\\"
						+ "DistributedEC2Sorting\\test_sort.txt"; 
				System.out.println("filename " + fileName);
				client.callSorter(fileName);

			} else if (line.equals("9")) {
				client.killer();
				System.out.println("Killed Server, Self Destruct!");
				System.exit(0);
			} else {
				System.out.println("Invalid option");
			}

			System.out.println();
		}
	}

}
