package client.file;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import datafile.DataRecord;
import utils.S3FileReader;

public class FileToDataRecords {

	public ArrayList<DataRecord> readRecordsFrom(String bucketName, String fileObjectKey){
	
		
		try {
			
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, fileObjectKey));
			GZIPInputStream gzipStream = new GZIPInputStream(s3object.getObjectContent());
			
			
			Reader decoder = new InputStreamReader(gzipStream, "ASCII");
			BufferedReader buffered = new BufferedReader(decoder);
			
			
			String fileLine;
			
			while((fileLine = buffered.readLine())!=null){
				
				// TODO: read line > csv > get offsets > get value > make DataRecord and return list
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to read file while converting to DataRecord");
			e.printStackTrace();
		}
		
		
		
		
		return null;
		
	}
	
}
