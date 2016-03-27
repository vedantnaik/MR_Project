package fileprocessing;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import datafile.DataRecord;
import utils.FileSystem;
import utils.S3FileReader;

public class DataRecordTester {

	public static void main(String[] args) {
		
		try {
			
			
			S3FileReader s3fr = new S3FileReader("cs6240sp16", "climate/199703hourly.txt.gz");
			
			
			
			
			DataRecord dr1 = new DataRecord("climate/199703hourly.txt.gz", 473, 607 - 473 - 1, 26.1);
			DataRecord dr2 = new DataRecord("climate/199703hourly.txt.gz", 743, 877 - 743 - 1, 25);
			DataRecord dr3 = new DataRecord("climate/199703hourly.txt.gz", 1413, 1545 - 1413 - 1, 25);
			
			
			
//			dr1.readRecordFrom(s3fr);
			System.out.println(dr1.readRecordFrom(s3fr));
			
			
//			dr2.readRecordFrom(s3fr);
			System.out.println(dr2.readRecordFrom(s3fr));
			
			System.out.println(dr3.readRecordFrom(s3fr));
			
			System.out.println(dr2.compareTo(dr1));
		
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
		

	}
	
}
