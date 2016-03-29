package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVParser;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import datafile.DataFileParser;
import datafile.DataRecord;

public class FileSystem {
	double summation = 0.0;
	int PARTS;
	
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
			
			// TODO: remove count condition to read all file
			while((fileLine = buffered.readLine())!=null /*&& count < 10*/){
				
				// TODO: read line > csv > get offsets > get value > make DataRecord and return list

				String[] fields = fileLine.split(",");
				
				if(DataFileParser.isRecordValid(fields)){
//					System.out.println(offset + "\t:" + fileLine);
//					System.out.println("\t\t\t DBT: " + DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP));
					
//					long recordFromOffset = offset;
//					int recordLength = fileLine.length();
//					sortValue
					
					double dryBulbTemp = Double.parseDouble(DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP));
					
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
	
	
	
	
	
	/****************************************************************
	 * 
	 * 						LOCAL FILE SYSTEM
	 * 
	 ****************************************************************/
	
	/**
	 * returns the List<File> given a directory location
	 * @param dir a string directory location 
	 * @return returns List<File> underneath that location/dir
	 * @throws Exception
	 */
	public List<FileType> getFiles(String dir) throws Exception {
		summation = 0.0;
		List<FileType> list = new ArrayList<>();
		addFile(new FileType(new File(dir)), list);
		return list;
	}
	/**
	 * recursive helper function for getFiles function
	 * to read the files beneath folder of folder.
	 * @param fileVar is the file a directory? if yes read more 
	 * and add to the list arg as argument
	 * @param list the list arg as argument
	 */
	private void addFile(FileType fileVar, List<FileType> list) {
		if (fileVar.file.isDirectory()) {
			String[] subNodes = fileVar.file.list();
			for (String subNode : subNodes) {
				addFile(new FileType(fileVar.file, subNode), list);
			}
		} else {
			list.add(fileVar);
			summation += fileVar.size;
		}
	}
	
	/**
	 * mega function to read files and call makePartition to make partitions
	 * of the data 
	 * @param the location of files to read from
	 * @param number the of partitions to part files into  
	 * @return List of List of partitions of equal size
	 * @throws Exception
	 */
	private List<List<FileType>> readFilesGetPartition(String location, int number) throws Exception {
		PARTS = number + 1;
		List<FileType> sortedFiles = getFiles(location);
		Collections.sort(sortedFiles);
		List<List<FileType>> partitions = makePartition(sortedFiles, PARTS);
		return partitions;
	}

	/**
	 * makes partition in number of numberOfParts based on size
	 * so each partition has almost equal size 
	 * @param sortedFiles files in sorted order
	 * @param numberOfParts number of parts in which all files are needed
	 * @return List of List of partitions of equal size
	 */
	private List<List<FileType>> makePartition(List<FileType> sortedFiles,
			int numberOfParts) {
		double eachPartSize = summation/numberOfParts;
		double eachPartSummation = 0.0;
		List<List<FileType>> partitions = new ArrayList<>();
		List<FileType> current = new ArrayList<>();
		for(FileType f : sortedFiles){			
			if(eachPartSummation >= eachPartSize){
				System.out.println("size " + eachPartSummation);
				eachPartSummation =  0.0;
				partitions.add(current);
				current = new ArrayList<>();
			}
			eachPartSummation += f.size;
			current.add(f);
		}
		return partitions;
	}

//	public static void main(String[] args) throws Exception {
//		FileSystem fs = new FileSystem();
//		System.out.println("files: " + fs.getFiles("X://climate//"));
//		System.out.println("summation: " + fs.summation);
//		System.out.println("sets: " + fs.readFilesGetPartition("X://climate//", 8));
//	}
}
