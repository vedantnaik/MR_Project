package coolmapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import utils.Constants;

public class Configuration implements Serializable{
	
	private HashMap<Integer, String> serverIPaddrMap;
	private HashMap<String, String> confMap = new HashMap<String, String>();
	
	
	
	private static final long serialVersionUID = 1L;
	private static String testString;
	
	
	
  	// read DNS files for cluster nodes in constructor
	// read
	

	public Configuration() {
		
		// Read and store public DNSs for further communication between EC2 instances
		serverIPaddrMap = new HashMap<Integer, String>();
		try {
			addIPaddrsFromPublicDnsFile();
			System.out.println("Read public DNS file");
			testString = "read";
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
	}

	
	
	
	public void set(String key, String value){
		confMap.put(key, value);
	}
	
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
				+ "confMap: " + confMap + "\n" + "test " + testString;
	}
	
}
