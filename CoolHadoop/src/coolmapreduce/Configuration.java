package coolmapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import utils.Constants;

public class Configuration {
	
	private static HashMap<Integer, String> serverIPaddrMap;
	
	// Input
	
	private static Path inputPath;
	private static Path outputPath;
	// TODO: confMap
	
	private static HashMap<String, String> confMap = new HashMap<String, String>();
	
	
	
	
	
	
	
	
  	// read DNS files for cluster nodes in constructor
	// read
	

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
	}

	
	
	
	public void set(String key, String value){
		confMap.put(key, value);
	}
	
	public String get(String key){
		return confMap.get(key);
	}
	
	
	

	// GETTER SETTERS

	public static HashMap<Integer, String> getServerIPaddrMap() {
		return serverIPaddrMap;
	}

	public static void setServerIPaddrMap(HashMap<Integer, String> serverIPaddrMap) {
		Configuration.serverIPaddrMap = serverIPaddrMap;
	}
	
	public static Path getInputPath() {
		return inputPath;
	}



	public static void setInputPath(Path inputPath) {
		Configuration.inputPath = inputPath;
	}



	public static Path getOutputPath() {
		return outputPath;
	}



	public static void setOutputPath(Path outputPath) {
		Configuration.outputPath = outputPath;
	}



	
	
}
