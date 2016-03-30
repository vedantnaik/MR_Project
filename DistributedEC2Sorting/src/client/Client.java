package client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import utils.FileSystem;

/**
 * A sample client which connects to the server and can issue a sorting command
 * with the contents of a string. It can also kill the server and self destruct
 * itself!
 *
 */
public class Client {
	static int[] ports = { 1210, 1211, 1212 };
	Socket[] sendingSocket = { null, null, null };
	DataOutputStream[] out = { null, null, null };
	BufferedReader[] inFromServer = { null, null, null };

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
	 */
	public void callSorter(String fileName, String serverIP) {
		
		FileSystem myS3FS = new FileSystem("cs6240sp16");
		
		HashMap<Integer, ArrayList<String>> partsMap = myS3FS.getS3Parts(ports.length); 
		
		try {

			for (int i = 0; i < ports.length; i++) {

				sendingSocket[i] = new Socket(serverIP, ports[i]);
				out[i] = new DataOutputStream(
						sendingSocket[i].getOutputStream());
				inFromServer[i] = new BufferedReader(new InputStreamReader(
						sendingSocket[i].getInputStream()));
				
				out[i].writeBytes("sort#start\n");
				
				for(String fileNameIter : partsMap.get(i)){
					out[i].writeBytes(fileNameIter+"\n");
				}
				
				out[i].writeBytes("sort#end\n");
				System.out.println("Running job on Server ..." + i);

			}

			// done phase
			int replies = 0;
			boolean[] replied = new boolean[ports.length];
			while (true) {
				for (int i = 0; i < replied.length && !replied[i]; i++) {

					String result = inFromServer[i].readLine();
					String[] returnedResult = result.split("#");
					System.out.println(returnedResult[0] + " Results from : "
							+ i);

					replied[i] = true;
					if (replied[i]) {
						replies++;
					}
				}
				if (replies == ports.length) {
					System.out.println("replied by " + replies);
					break;
				}

			}
			// 3 people replied
			System.out.println("replied array " + Arrays.toString(replied));

			System.out.println("sending command to distribute now!");
			// distribute pivots
			for (int i = 0; i < ports.length; i++) {
				out[i].writeBytes("distribute#All" + "\n");
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
	private void killer(String serverIP) {

		try {

			for (int i = 0; i < ports.length; i++) {
				Socket sendingSocket = new Socket(serverIP, ports[i]);
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
			System.out.println("Usage Client 127.0.0.1");
			System.exit(0);
		}
		BufferedReader din = new BufferedReader(
				new InputStreamReader(System.in));
		String serverIP = args[0];

		Client client = new Client();
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
				client.callSorter(fileName, serverIP);

			} else if (line.equals("9")) {
				client.killer(serverIP);
				System.out.println("Killed Server, Self Destruct!");
				System.exit(0);
			} else {
				System.out.println("Invalid option");
			}

			System.out.println();
		}
	}

}
