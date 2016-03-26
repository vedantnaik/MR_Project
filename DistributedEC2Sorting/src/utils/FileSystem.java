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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class FileSystem {
	double summation = 0.0;
	int PARTS;
	
	// S3
	AWSCredentials credentials;
	AmazonS3 s3client;
	
	public FileSystem() {
		
		this.credentials = new ProfileCredentialsProvider().getCredentials();
		this.s3client = new AmazonS3Client(credentials);
		
	}
	
	/*
	 * Get list of all buckets:
	  	for (Bucket bucket : s3client.listBuckets()) {
			System.out.println(" - " + bucket.getName());
		}
	 * 
	 * Create Bucket:
	 	String bucketName = "javatutorial-net-example-bucket";
		s3client.createBucket(bucketName);
	 * 
	 * For more snippets: https://javatutorial.net/java-s3-example
	 * */
	
	
	
	
	/****************************************************************
	 * 
	 * 						AMAZON (S3) FILE SYSTEM
	 * 
	 ****************************************************************/
	
	/**
	 * returns a Reader class, pointing to beginning of file 
	 * 			for to a given file in the given s3 bucket
	 * @param dir a string directory location 
	 * @return returns List<File> underneath that location/dir
	 * @throws IOException 
	 * @throws Exception
	 * */
	
	public Reader getFileReader(String bucketName, String fileObjectKey) throws IOException{
		
		S3Object s3object = this.s3client.getObject(new GetObjectRequest(bucketName, fileObjectKey));
		InputStream objectData = s3object.getObjectContent();
		// Should work the same as:
		// InputStream gzipStream = new GZIPInputStream(new FileInputStream(this.fileName));
		
		
		Reader decoder = new InputStreamReader(objectData, "ASCII");
		
		objectData.close();
		s3object = null;
		
		return decoder;
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

	public static void main(String[] args) throws Exception {
		FileSystem fs = new FileSystem();
		System.out.println("files: " + fs.getFiles("X://climate//"));
		System.out.println("summation: " + fs.summation);
		System.out.println("sets: " + fs.readFilesGetPartition("X://climate//", 8));
	}
}
