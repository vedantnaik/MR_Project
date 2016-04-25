package coolmapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import utils.Constants;
/**
 * 
 * @author Vedant_Naik, Dixit_Patel, Vaibhav_Tyagi
 *
 */
public class Configuration implements Serializable{
	
	private HashMap<Integer, String> serverIPaddrMap;
	private HashMap<String, String> confMap = new HashMap<String, String>();
	
	private static final long serialVersionUID = 1L;
	
	public Configuration() {
		// Read and store public DNSs for further communication between EC2 instances
		serverIPaddrMap = new HashMap<Integer, String>();
		try {
			addIPaddrsFromPublicDnsFile();
			System.out.println("Read public DNS file");
		} catch (IOException e) {
			System.err.println("Unable to read file " + Constants.PUBLIC_DNS_FILE);
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor helper to read IPs of all servers into Map
	 * @throws IOException 
	 * */
	private void addIPaddrsFromPublicDnsFile() throws IOException {
		
		BufferedReader br = new BufferedReader(new FileReader(Constants.PUBLIC_DNS_FILE));
		String line;
		int serverNumber = 0;
		while((line = br.readLine()) != null){
			serverIPaddrMap.put(serverNumber++, line.trim());
		}
		br.close();
		
		confMap.put(Constants.MASTER_SERVER_IP_KEY, 
					serverIPaddrMap.get(serverIPaddrMap.size() - 1));

		serverIPaddrMap.remove(serverIPaddrMap.size() - 1);
	}
	
	/**
	 * Set a custom key value pair in the confMap
	 * */
	public void set(String key, String value){
		confMap.put(key, value);
	}
	
	/**
	 * Get a custom stored key value pair from the confMap
	 * */
	public String get(String key){
		return confMap.get(key);
	}

	// GETTER SETTERS
	public HashMap<Integer, String> getServerIPaddrMap() {
		return serverIPaddrMap;
	}

	public void setServerIPaddrMap(HashMap<Integer, String> _serverIPaddrMap) {
		serverIPaddrMap = _serverIPaddrMap;
	}
	
	@Override
	public String toString() {
		return "serverIPaddrMap: " + serverIPaddrMap + "\n" 
				+ "confMap: " + confMap + "\n";
	}
}
