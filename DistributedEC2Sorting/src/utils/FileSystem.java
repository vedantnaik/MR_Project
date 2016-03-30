package utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import datafile.DataFileParser;
import datafile.DataRecord;

public class FileSystem {
	double summation = 0.0;
	int PARTS;
	
	String bucketName;
	String fileObjectKey;
	ArrayList<String> fileNameSizeList;
	
	
	public FileSystem(String bucketName){
		this.bucketName = bucketName;
		
		this.fileNameSizeList = new ArrayList<String>();
		
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		ObjectListing objList = s3client.listObjects("cs6240sp16");
		
		
		// TODO: REMOVE AFTER DEBUG!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		int debug_limitCount = 0;
		
		for(S3ObjectSummary objSum : objList.getObjectSummaries() ){
			if(objSum.getKey().contains("climate") && objSum.getKey().contains("txt.gz")){
				this.fileNameSizeList.add(objSum.getKey() + ":" + objSum.getSize());
				debug_limitCount++;
			}
			if(debug_limitCount == 6) {break;}
		}
		
	}
	
	
	
	/****************************************************************
	 * 
	 * 						AMAZON (S3) FILE SYSTEM
	 * 
	 ****************************************************************/
	
	public static ArrayList<DataRecord> readRecordsFrom(String bucketName, String fileObjectKey){
	
		ArrayList<DataRecord> dataRecordList = new ArrayList<DataRecord>();
		
		try {
			
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, fileObjectKey));
			GZIPInputStream gzipStream = new GZIPInputStream(s3object.getObjectContent());
			Reader decoder = new InputStreamReader(gzipStream, "ASCII");
			BufferedReader buffered = new BufferedReader(decoder);
			
			String fileLine;
			
			int count = 0;
			
			String header = fileLine = buffered.readLine();
			
			int offset = header.length() + 1;
			
			// TODO: REMOVE AFTER DEBUG!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
			
			// TODO: remove count condition to read all file
			while((fileLine = buffered.readLine())!=null && count < 5){
				
				// TODO: read line > csv > get offsets > get value > make DataRecord and return list

				String[] fields = fileLine.split(",");
				
				if(DataFileParser.isRecordValid(fields)){
//					System.out.println(offset + "\t:" + fileLine);
//					System.out.println("\t\t\t DBT: " + DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP));
					
//					long recordFromOffset = offset;
//					int recordLength = fileLine.length();
//					sortValue
					
					double dryBulbTemp = Double.parseDouble(DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP));
					
					System.out.println("=========================================================");
					System.out.println("read record \t\t" + dryBulbTemp);
					System.out.println("=========================================================");
					
					dataRecordList.add(new DataRecord(fileObjectKey, offset, fileLine.length(), dryBulbTemp));
				}
				offset = offset + fileLine.length() + 1;
				count++;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to read file while converting to DataRecord");
			e.printStackTrace();
		}
		
		return dataRecordList;
	}
	
	public HashMap<Integer, ArrayList<String>> getS3Parts(int parts){
		
		Collections.sort(this.fileNameSizeList, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				int val1 = Integer.parseInt(o1.split(":")[1]);
				int val2 = Integer.parseInt(o2.split(":")[1]);
				if(val1 > val2) {return 1;}
				if(val1 < val2) {return -1;}
				return 0;
			}
		});
		
		HashMap<Integer, ArrayList<String>> partitionMap = new HashMap<Integer, ArrayList<String>>(); 
	
		int spinner = 0;
		for (String fileNameSize : fileNameSizeList){
			if(spinner == parts) {spinner = 0;}
			
			if(!partitionMap.containsKey(spinner)){
				partitionMap.put(spinner, new ArrayList<String>());
			}
			partitionMap.get(spinner).add(fileNameSize.split(":")[0].trim());
			
			spinner++;
		}
		
		return partitionMap;
	}


	
	/*
	 * 			WRITE TO EC2 LOCAL DISK
	 * 
	 * */

	public static void writeToEC2(List<DataRecord> drsPartListToBeWritten, int serverNumber_p, int fromServer_s) throws IOException {
		
		String fileName = fromServer_s+"/"+"s"+fromServer_s+"p"+serverNumber_p+".txt";
		
		File f = null;
		
		f = new File(fromServer_s + "");
		f.mkdir();
		
		FileOutputStream fout = new FileOutputStream(fileName);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(drsPartListToBeWritten);

	}

}
